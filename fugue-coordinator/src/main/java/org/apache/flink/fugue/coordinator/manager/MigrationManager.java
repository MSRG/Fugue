/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fugue.coordinator.manager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages.*;
import org.apache.flink.fugue.common.state.MigrationContext;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Migration Manager that coordinates the two-phase migration protocol.
 * Manages the state machine transitions and RPC communication with TaskManagers.
 */
public class MigrationManager {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationManager.class);

    /**
     * Default port a target controller's networked state-transfer listener binds to. Deployments that
     * cannot use a fixed port bind an ephemeral one and report it back through the migration plan.
     */
    private static final int DEFAULT_STATE_TRANSFER_PORT = 8888;

    /** The job this manager is responsible for. */
    private final JobID jobId;

    /** Map of migration ID to migration context. */
    private final Map<Long, MigrationContext> activeMigrations;

    /** Map of partition ID to current migration (ensures no overlapping migrations). */
    private final Map<Integer, Long> partitionToMigration;

    /** Lock for thread-safe access. */
    private final ReentrantReadWriteLock lock;

    /** Executor for async operations. */
    private final ScheduledExecutorService executor;

    /** RPC gateway for communication with TaskManagers. */
    private final MigrationRpcGateway rpcGateway;

    /** Configuration for migration parameters. */
    private PolicyConfiguration configuration;

    /** Counter for generating unique migration IDs. */
    private final AtomicLong migrationIdCounter;

    /** Callback for barrier injection. */
    private BarrierInjectionCallback barrierCallback;

    /** Active timeout timers per migration (cancelled when the migration reaches a terminal state). */
    private final Map<Long, List<ScheduledFuture<?>>> timeoutTimers;

    /** Per-migration completion futures, completed with the terminal state (COMMITTED/ABORTED). */
    private final Map<Long, CompletableFuture<MigrationState>> completionFutures;

    public MigrationManager(JobID jobId, MigrationRpcGateway rpcGateway) {
        this.jobId = jobId;
        this.activeMigrations = new ConcurrentHashMap<>();
        this.partitionToMigration = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(4, r -> {
            Thread t = new Thread(r);
            t.setName("fugue-migration-manager-" + jobId);
            t.setDaemon(true);
            return t;
        });
        // Drop pending delayed timeout tasks promptly on shutdown, and reclaim cancelled timers.
        exec.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        exec.setRemoveOnCancelPolicy(true);
        this.executor = exec;
        this.rpcGateway = rpcGateway;
        this.configuration = new PolicyConfiguration();
        this.migrationIdCounter = new AtomicLong(0);
        this.timeoutTimers = new ConcurrentHashMap<>();
        this.completionFutures = new ConcurrentHashMap<>();
    }

    /**
     * Start new migrations for the given plans.
     */
    public CompletableFuture<List<Long>> startMigrations(List<MigrationPlan> plans) {
        lock.writeLock().lock();
        try {
            List<CompletableFuture<Long>> futures = new ArrayList<>();

            for (MigrationPlan plan : plans) {
                // Check if partition is already being migrated
                if (partitionToMigration.containsKey(plan.getPartitionId())) {
                    LOG.warn("Partition {} is already being migrated", plan.getPartitionId());
                    futures.add(CompletableFuture.completedFuture(-1L));
                    continue;
                }

                long migrationId = migrationIdCounter.incrementAndGet();
                MigrationContext context = new MigrationContext(plan, migrationId);

                activeMigrations.put(migrationId, context);
                partitionToMigration.put(plan.getPartitionId(), migrationId);
                completionFutures.put(migrationId, new CompletableFuture<>());

                CompletableFuture<Long> future = initiateMigration(context);
                futures.add(future);

                LOG.info("Started migration {} for partition {}: {} -> {}",
                        migrationId, plan.getPartitionId(),
                        plan.getSourceInstance().getSubtaskIndex(),
                        plan.getTargetInstance().getSubtaskIndex());
            }

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futures.stream()
                            .map(CompletableFuture::join)
                            .filter(id -> id > 0)
                            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Initiate a migration by transitioning to INIT state.
     */
    private CompletableFuture<Long> initiateMigration(MigrationContext context) {
        if (!context.transitionTo(MigrationState.INIT)) {
            LOG.error("Failed to transition migration {} to INIT state", context.getMigrationId());
            return CompletableFuture.completedFuture(-1L);
        }

        // Set up timeout timer
        long timeout = configuration.getLong("migration.init.timeout", 30000L);
        ScheduledFuture<?> timer = executor.schedule(
                () -> handleTimeout(context.getMigrationId(), MigrationState.INIT),
                timeout, TimeUnit.MILLISECONDS);
        trackTimeout(context.getMigrationId(), timer);

        // Send prepare messages to source and target
        CompletableFuture<Void> sourcePrepare = prepareSource(context);
        CompletableFuture<Void> targetPrepare = prepareTarget(context);

        return CompletableFuture.allOf(sourcePrepare, targetPrepare)
                .thenCompose(v -> {
                    cancelTimeout(context.getMigrationId());
                    return startPreCopy(context);
                })
                .thenApply(v -> context.getMigrationId())
                .exceptionally(throwable -> {
                    LOG.error("Migration {} failed during initialization",
                            context.getMigrationId(), throwable);
                    abortMigration(context.getMigrationId(), throwable.getMessage());
                    return -1L;
                });
    }

    /**
     * Prepare source operator for migration.
     */
    private CompletableFuture<Void> prepareSource(MigrationContext context) {
        MigrationPlan plan = context.getMigrationPlan();

        String targetAddress = plan.getTargetInstance().getTaskManagerLocation();
        int targetPort = DEFAULT_STATE_TRANSFER_PORT;

        PrepareMigrationSource message = new PrepareMigrationSource(
                context.getMigrationId(),
                plan,
                targetAddress,
                targetPort);

        return rpcGateway.sendToTaskManager(
                plan.getSourceInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> {
                    if (response instanceof MigrationResponse) {
                        MigrationResponse resp = (MigrationResponse) response;
                        if (!resp.isSuccess()) {
                            throw new RuntimeException("Failed to prepare source: " + resp.getMessage());
                        }
                    }
                });
    }

    /**
     * Prepare target operator for migration.
     */
    private CompletableFuture<Void> prepareTarget(MigrationContext context) {
        MigrationPlan plan = context.getMigrationPlan();
        int listenPort = DEFAULT_STATE_TRANSFER_PORT;

        PrepareMigrationTarget message = new PrepareMigrationTarget(
                context.getMigrationId(),
                plan,
                listenPort);

        return rpcGateway.sendToTaskManager(
                plan.getTargetInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> {
                    if (response instanceof MigrationResponse) {
                        MigrationResponse resp = (MigrationResponse) response;
                        if (!resp.isSuccess()) {
                            throw new RuntimeException("Failed to prepare target: " + resp.getMessage());
                        }
                    }
                });
    }

    /**
     * Start pre-copy phase.
     */
    private CompletableFuture<Void> startPreCopy(MigrationContext context) {
        if (!context.transitionTo(MigrationState.PRE_COPY)) {
            LOG.error("Failed to transition migration {} to PRE_COPY state", context.getMigrationId());
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Invalid state transition"));
        }

        LOG.info("Starting pre-copy phase for migration {}", context.getMigrationId());

        // Get rate limit from configuration
        long rateLimitMbps = configuration.getLong(
                PolicyConfiguration.NETWORK_BANDWIDTH_LIMIT, 100L);
        long rateLimitBytesPerSec = rateLimitMbps * 1024 * 1024;

        StartPreCopy message = new StartPreCopy(
                context.getMigrationId(),
                context.getMigrationPlan().getPartitionId(),
                rateLimitBytesPerSec);

        return rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> scheduleConvergenceTimeout(context));
    }

    /**
     * Schedule an abort if the pre-copy phase fails to converge within the configured time. The
     * convergence and divergence decisions themselves are event-driven, made in
     * {@link #handlePreCopyRoundComplete} when the source reports each round; this is only the
     * safety timeout.
     */
    private void scheduleConvergenceTimeout(MigrationContext context) {
        long timeout = configuration.getLong("migration.precopy.timeout", 300000L);
        ScheduledFuture<?> timer = executor.schedule(
                () -> {
                    if (context.getCurrentState() == MigrationState.PRE_COPY) {
                        abortMigration(context.getMigrationId(), "pre-copy convergence timeout");
                    }
                },
                timeout,
                TimeUnit.MILLISECONDS);
        trackTimeout(context.getMigrationId(), timer);
    }

    /**
     * Inject migration barrier into the dataflow.
     */
    private void injectBarrier(MigrationContext context) {
        LOG.info("Injecting barrier for migration {}", context.getMigrationId());

        context.setBarrierInjected(true);

        if (barrierCallback != null) {
            MigrationBarrier barrier = MigrationBarrier.createStandalone(
                    context.getMigrationId(),
                    Collections.singletonList(context.getMigrationPlan()));

            barrierCallback.injectBarrier(barrier);
        }

        // Safety timeout: abort if the barrier never aligns. The FINALIZING transition itself is
        // event-driven via onBarrierAligned(...), invoked by the runtime when alignment completes.
        long timeout = configuration.getLong("migration.barrier.timeout", 60000L);
        ScheduledFuture<?> timer = executor.schedule(
                () -> {
                    if (context.getCurrentState() == MigrationState.AWAIT_BARRIER) {
                        abortMigration(context.getMigrationId(), "barrier alignment timeout");
                    }
                },
                timeout,
                TimeUnit.MILLISECONDS);
        trackTimeout(context.getMigrationId(), timer);
    }

    /**
     * Invoked by the runtime when the migration barrier has aligned across all participating
     * operator instances. Drives AWAIT_BARRIER -&gt; FINALIZING and requests the final delta.
     */
    public void onBarrierAligned(long migrationId) {
        MigrationContext context = activeMigrations.get(migrationId);
        if (context == null) {
            LOG.warn("Barrier alignment for unknown/cleared migration {}", migrationId);
            return;
        }
        if (context.getCurrentState() == MigrationState.AWAIT_BARRIER
                && context.transitionTo(MigrationState.FINALIZING)) {
            scheduleFinalizationTimeout(context);
            transferFinalDelta(context);
        }
    }

    /**
     * Schedule an abort if finalization (final delta + commit) does not complete in time.
     */
    private void scheduleFinalizationTimeout(MigrationContext context) {
        long timeout = configuration.getLong("migration.finalization.timeout", 60000L);
        ScheduledFuture<?> timer = executor.schedule(
                () -> {
                    if (context.getCurrentState() == MigrationState.FINALIZING) {
                        abortMigration(context.getMigrationId(), "finalization timeout");
                    }
                },
                timeout,
                TimeUnit.MILLISECONDS);
        trackTimeout(context.getMigrationId(), timer);
    }

    /**
     * Transfer final delta after barrier alignment.
     */
    private void transferFinalDelta(MigrationContext context) {
        LOG.info("Transferring final delta for migration {}", context.getMigrationId());

        TransferFinalDelta message = new TransferFinalDelta(
                context.getMigrationId(),
                context.getMigrationPlan().getPartitionId());

        rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                message)
                .thenAccept(response -> {
                    context.setFinalDeltaSent(true);
                    commitMigration(context);
                })
                .exceptionally(throwable -> {
                    LOG.error("Failed to transfer final delta for migration {}",
                            context.getMigrationId(), throwable);
                    abortMigration(context.getMigrationId(), throwable.getMessage());
                    return null;
                });
    }

    /**
     * Commit the migration.
     */
    private void commitMigration(MigrationContext context) {
        if (!context.transitionTo(MigrationState.COMMITTED)) {
            LOG.error("Failed to transition migration {} to COMMITTED state",
                    context.getMigrationId());
            return;
        }

        LOG.info("Migration {} committed successfully in {}ms",
                context.getMigrationId(), context.getDuration());

        // Clean up
        finalizeMigration(context.getMigrationId(), true);

        // Send commit messages to source and target
        CommitMigration message = new CommitMigration(
                context.getMigrationId(),
                context.getMigrationPlan().getPartitionId());

        rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                message);
        rpcGateway.sendToTaskManager(
                context.getMigrationPlan().getTargetInstance().getTaskManagerLocation(),
                message);
    }

    /**
     * Abort a migration.
     */
    public void abortMigration(long migrationId, String reason) {
        lock.writeLock().lock();
        try {
            MigrationContext context = activeMigrations.get(migrationId);
            if (context == null) {
                return;
            }

            context.forceTransitionTo(MigrationState.ABORTED);
            context.setLastError(reason);

            LOG.warn("Migration {} aborted: {}", migrationId, reason);

            // Send abort messages
            AbortMigration message = new AbortMigration(migrationId, reason);

            rpcGateway.sendToTaskManager(
                    context.getMigrationPlan().getSourceInstance().getTaskManagerLocation(),
                    message);
            rpcGateway.sendToTaskManager(
                    context.getMigrationPlan().getTargetInstance().getTaskManagerLocation(),
                    message);

            finalizeMigration(migrationId, false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle RPC message from TaskManager.
     */
    public void handleMessage(MigrationMessage message) {
        long migrationId = message.getMigrationId();
        MigrationContext context = activeMigrations.get(migrationId);

        if (context == null) {
            LOG.warn("Received message for unknown migration: {}", migrationId);
            return;
        }

        if (message instanceof PreCopyRoundComplete) {
            handlePreCopyRoundComplete(context, (PreCopyRoundComplete) message);
        } else if (message instanceof FinalDeltaComplete) {
            handleFinalDeltaComplete(context, (FinalDeltaComplete) message);
        }
    }

    /**
     * Handle pre-copy round completion.
     */
    private void handlePreCopyRoundComplete(MigrationContext context, PreCopyRoundComplete message) {
        LOG.info("Pre-copy round {} complete for migration {}, deltaSize={}, converged={}",
                message.getRoundNumber(), context.getMigrationId(),
                message.getDeltaSize(), message.isConverged());

        context.incrementPreCopyRound();
        context.updateDeltaSize(message.getDeltaSize());
        context.addBytesTransferred(message.getBytesTransferred());

        if (context.getCurrentState() != MigrationState.PRE_COPY) {
            return;
        }

        // Convergence: the source reports the delta is below threshold -> proceed to cutover.
        if (message.isConverged()) {
            if (context.transitionTo(MigrationState.AWAIT_BARRIER)) {
                injectBarrier(context);
            }
            return;
        }

        // Divergence safeguard: if the delta log fails to shrink, abort.
        int maxNonDecreasing =
                configuration.getInt("migration.precopy.max-non-decreasing-rounds", 3);
        if (context.recordDeltaAndCheckDivergence(message.getDeltaSize(), maxNonDecreasing)) {
            abortMigration(
                    context.getMigrationId(),
                    "pre-copy divergence: delta log not shrinking (R_dirty >= R_net)");
            return;
        }

        // Bound the number of pre-copy rounds.
        int maxRounds = configuration.getInt(PolicyConfiguration.MAX_PRECOPY_ROUNDS, 10);
        if (context.getPreCopyRounds() >= maxRounds) {
            abortMigration(
                    context.getMigrationId(),
                    "pre-copy did not converge within " + maxRounds + " rounds");
        }
    }

    /**
     * Handle final delta completion.
     */
    private void handleFinalDeltaComplete(MigrationContext context, FinalDeltaComplete message) {
        LOG.info("Final delta complete for migration {}, size={}",
                context.getMigrationId(), message.getFinalDeltaSize());

        context.addBytesTransferred(message.getFinalDeltaSize());

        if (context.getCurrentState() == MigrationState.FINALIZING) {
            commitMigration(context);
        }
    }

    /**
     * Handle timeout for a migration state.
     */
    private void handleTimeout(long migrationId, MigrationState state) {
        MigrationContext context = activeMigrations.get(migrationId);
        if (context != null && context.getCurrentState() == state) {
            LOG.error("Migration {} timed out in state {}", migrationId, state);
            abortMigration(migrationId, "Timeout in state " + state);
        }
    }

    /** Track a timeout timer so it can be cancelled when the migration reaches a terminal state. */
    private void trackTimeout(long migrationId, ScheduledFuture<?> timer) {
        timeoutTimers.computeIfAbsent(migrationId, k -> new CopyOnWriteArrayList<>()).add(timer);
    }

    /**
     * Cancel all outstanding timeout timers for a migration.
     */
    private void cancelTimeout(long migrationId) {
        List<ScheduledFuture<?>> timers = timeoutTimers.remove(migrationId);
        if (timers != null) {
            for (ScheduledFuture<?> timer : timers) {
                timer.cancel(false);
            }
        }
    }

    /**
     * Finalize a migration (cleanup).
     */
    private void finalizeMigration(long migrationId, boolean success) {
        lock.writeLock().lock();
        try {
            MigrationContext context = activeMigrations.remove(migrationId);
            if (context != null) {
                partitionToMigration.remove(context.getMigrationPlan().getPartitionId());
                cancelTimeout(migrationId);
            }
            CompletableFuture<MigrationState> completion = completionFutures.remove(migrationId);
            if (completion != null) {
                completion.complete(success ? MigrationState.COMMITTED : MigrationState.ABORTED);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get active migrations.
     */
    public Collection<MigrationContext> getActiveMigrations() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(activeMigrations.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get migration by ID.
     */
    public MigrationContext getMigration(long migrationId) {
        return activeMigrations.get(migrationId);
    }

    /**
     * Returns a future that completes with the terminal state (COMMITTED or ABORTED) when the given
     * migration finishes. The future is created when the migration starts; capture it before the
     * migration completes.
     */
    public CompletableFuture<MigrationState> getCompletion(long migrationId) {
        return completionFutures.get(migrationId);
    }

    /**
     * Set barrier injection callback.
     */
    public void setBarrierCallback(BarrierInjectionCallback callback) {
        this.barrierCallback = callback;
    }

    /**
     * Update configuration.
     */
    public void updateConfiguration(PolicyConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Shutdown the manager.
     */
    public void shutdown() {
        // Abort all active migrations
        lock.writeLock().lock();
        try {
            for (MigrationContext context : activeMigrations.values()) {
                abortMigration(context.getMigrationId(), "Manager shutdown");
            }
        } finally {
            lock.writeLock().unlock();
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Callback interface for barrier injection.
     */
    public interface BarrierInjectionCallback {
        void injectBarrier(MigrationBarrier barrier);
    }

    /**
     * RPC gateway interface for TaskManager communication.
     */
    public interface MigrationRpcGateway {
        CompletableFuture<MigrationMessage> sendToTaskManager(String taskManagerLocation, MigrationMessage message);
    }
}