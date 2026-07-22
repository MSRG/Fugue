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

package org.apache.flink.fugue.runtime.controller;

import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages.*;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.fugue.runtime.transfer.StateMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Migration Controller — the local (per-TaskManager) agent that handles the coordinator's RPC commands
 * (prepare / start-pre-copy / barrier / finalize / commit / abort) and drives the protocol against a
 * pluggable {@link StateMigrationService}. The state-transfer mechanism is supplied at the operator
 * level (where the live RocksDB keyed backend is reachable; see {@link StateMigrationService}); when
 * none is injected the control plane still works but state-transfer calls fail loudly rather than
 * silently moving disconnected state.
 */
public class MigrationController {
    private static final Logger LOG = LoggerFactory.getLogger(MigrationController.class);

    /** Pre-copy converges once a round's residual delta falls below this many bytes. */
    private static final long CONVERGENCE_THRESHOLD_BYTES = 5L * 1024 * 1024;

    /** The task manager location. */
    private final String taskManagerLocation;

    /** Pluggable state-transfer mechanism, supplied at the operator level (see {@link StateMigrationService}). */
    private final StateMigrationService stateTransferService;

    /** Active migrations (source-side). */
    private final Map<Long, SourceMigrationContext> sourceMigrations;

    /** Active migrations (target-side). */
    private final Map<Long, TargetMigrationContext> targetMigrations;

    /** Executor for async operations. */
    private final ExecutorService executor;

    /** Callback to coordinator. */
    private CoordinatorCallback coordinatorCallback;

    public MigrationController(String taskManagerLocation) {
        this(taskManagerLocation, new UnconfiguredStateMigration());
    }

    public MigrationController(
            String taskManagerLocation, StateMigrationService stateMigrationService) {
        this.taskManagerLocation = taskManagerLocation;
        this.stateTransferService = stateMigrationService;
        this.sourceMigrations = new ConcurrentHashMap<>();
        this.targetMigrations = new ConcurrentHashMap<>();
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("fugue-migration-controller");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Handle prepare migration source message.
     */
    public CompletableFuture<MigrationResponse> handlePrepareMigrationSource(
            PrepareMigrationSource message) {

        long migrationId = message.getMigrationId();
        LOG.info("Preparing migration source for migration {}", migrationId);

        return CompletableFuture.supplyAsync(() -> {
            try {
                MigrationPlan plan = message.getMigrationPlan();

                // Create source migration context
                SourceMigrationContext context = new SourceMigrationContext(
                        migrationId,
                        plan,
                        message.getTargetAddress(),
                        message.getTargetPort());

                sourceMigrations.put(migrationId, context);

                // Initialize state transfer service for this migration
                stateTransferService.prepareSource(
                        migrationId,
                        plan.getPartitionId(),
                        message.getTargetAddress(),
                        message.getTargetPort());

                LOG.info("Source prepared for migration {}", migrationId);
                return new MigrationResponse(migrationId, true, "Source prepared");

            } catch (Exception e) {
                LOG.error("Failed to prepare source for migration {}", migrationId, e);
                return new MigrationResponse(migrationId, false,
                        "Failed to prepare source: " + e.getMessage());
            }
        }, executor);
    }

    /**
     * Handle prepare migration target message.
     */
    public CompletableFuture<MigrationResponse> handlePrepareMigrationTarget(
            PrepareMigrationTarget message) {

        long migrationId = message.getMigrationId();
        LOG.info("Preparing migration target for migration {}", migrationId);

        return CompletableFuture.supplyAsync(() -> {
            try {
                MigrationPlan plan = message.getMigrationPlan();

                // Create target migration context
                TargetMigrationContext context = new TargetMigrationContext(
                        migrationId,
                        plan,
                        message.getListenPort());

                targetMigrations.put(migrationId, context);

                // Initialize state transfer service for this migration
                stateTransferService.prepareTarget(
                        migrationId,
                        plan.getPartitionId(),
                        message.getListenPort());

                LOG.info("Target prepared for migration {}", migrationId);
                return new MigrationResponse(migrationId, true, "Target prepared");

            } catch (Exception e) {
                LOG.error("Failed to prepare target for migration {}", migrationId, e);
                return new MigrationResponse(migrationId, false,
                        "Failed to prepare target: " + e.getMessage());
            }
        }, executor);
    }

    /**
     * Handle start pre-copy message.
     */
    public CompletableFuture<MigrationResponse> handleStartPreCopy(StartPreCopy message) {
        long migrationId = message.getMigrationId();
        LOG.info("Starting pre-copy for migration {}", migrationId);

        SourceMigrationContext context = sourceMigrations.get(migrationId);
        if (context == null) {
            return CompletableFuture.completedFuture(
                    new MigrationResponse(migrationId, false, "Unknown migration"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                context.setState(MigrationState.PRE_COPY);

                // Start iterative pre-copy rounds
                startPreCopyRounds(context, message.getRateLimitBytesPerSec());

                return new MigrationResponse(migrationId, true, "Pre-copy started");

            } catch (Exception e) {
                LOG.error("Failed to start pre-copy for migration {}", migrationId, e);
                return new MigrationResponse(migrationId, false,
                        "Failed to start pre-copy: " + e.getMessage());
            }
        }, executor);
    }

    /**
     * Execute iterative pre-copy rounds.
     */
    private void startPreCopyRounds(SourceMigrationContext context, long rateLimit) {
        executor.submit(() -> {
            long migrationId = context.getMigrationId();
            int partitionId = context.getPlan().getPartitionId();
            int roundNumber = 0;
            long convergenceThreshold = CONVERGENCE_THRESHOLD_BYTES;

            try {
                while (context.getState() == MigrationState.PRE_COPY) {
                    roundNumber++;
                    LOG.info("Starting pre-copy round {} for migration {}", roundNumber, migrationId);

                    // Transfer state snapshot/delta
                    StateMigrationService.TransferResult result =
                            stateTransferService.transferSnapshot(
                                    migrationId,
                                    partitionId,
                                    roundNumber,
                                    rateLimit);

                    context.incrementRound();
                    context.addBytesTransferred(result.getBytesTransferred());
                    context.setCurrentDeltaSize(result.getDeltaSize());

                    // Check convergence
                    boolean converged = result.getDeltaSize() < convergenceThreshold;

                    // Notify coordinator
                    if (coordinatorCallback != null) {
                        PreCopyRoundComplete notification = new PreCopyRoundComplete(
                                migrationId,
                                roundNumber,
                                result.getDeltaSize(),
                                result.getBytesTransferred(),
                                converged);

                        coordinatorCallback.sendToCoordinator(notification);
                    }

                    if (converged) {
                        LOG.info("Pre-copy converged for migration {} after {} rounds",
                                migrationId, roundNumber);
                        context.setState(MigrationState.AWAIT_BARRIER);
                        break;
                    }

                    // Wait before next round
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Pre-copy interrupted for migration {}", migrationId);
            } catch (Exception e) {
                LOG.error("Pre-copy failed for migration {}", migrationId, e);
                context.setState(MigrationState.ABORTED);
            }
        });
    }

    /**
     * Handle migration barrier arrival.
     */
    public void handleBarrierArrival(MigrationBarrier barrier) {
        LOG.info("Migration barrier arrived: {}", barrier);

        for (MigrationPlan plan : barrier.getMigrationPlans()) {
            int partitionId = plan.getPartitionId();

            // Handle on source side
            long sourceMigrationId = findSourceMigration(partitionId);
            if (sourceMigrationId > 0) {
                SourceMigrationContext sourceCtx = sourceMigrations.get(sourceMigrationId);
                if (sourceCtx != null && sourceCtx.getState() == MigrationState.AWAIT_BARRIER) {
                    handleSourceBarrier(sourceCtx);
                }
            }

            // Handle on target side
            long targetMigrationId = findTargetMigration(partitionId);
            if (targetMigrationId > 0) {
                TargetMigrationContext targetCtx = targetMigrations.get(targetMigrationId);
                if (targetCtx != null) {
                    handleTargetBarrier(targetCtx, barrier);
                }
            }
        }
    }

    /**
     * Handle barrier on source side.
     */
    private void handleSourceBarrier(SourceMigrationContext context) {
        long migrationId = context.getMigrationId();
        LOG.info("Handling source barrier for migration {}", migrationId);

        executor.submit(() -> {
            try {
                context.setState(MigrationState.FINALIZING);

                // Stop accepting new writes to delta log
                stateTransferService.stopDeltaLogging(migrationId);

                // Wait for transfer final delta command from coordinator

            } catch (Exception e) {
                LOG.error("Failed to handle source barrier for migration {}", migrationId, e);
            }
        });
    }

    /**
     * Handle barrier on target side.
     */
    private void handleTargetBarrier(TargetMigrationContext context, MigrationBarrier barrier) {
        long migrationId = context.getMigrationId();
        int partitionId = context.getPlan().getPartitionId();
        LOG.info("Handling target barrier for migration {}", migrationId);

        executor.submit(() -> {
            try {
                // Post-barrier record buffering/replay is performed at the operator level
                // (FugueKeyedStateOperator/FugueCutover); the controller only records that the
                // barrier was observed for this migration.
                context.setBarrierReceived(true);

                LOG.info("Target barrier observed for migration {}", migrationId);

            } catch (Exception e) {
                LOG.error("Failed to handle target barrier for migration {}", migrationId, e);
            }
        });
    }

    /**
     * Handle transfer final delta message.
     */
    public CompletableFuture<MigrationResponse> handleTransferFinalDelta(
            TransferFinalDelta message) {

        long migrationId = message.getMigrationId();
        LOG.info("Transferring final delta for migration {}", migrationId);

        SourceMigrationContext context = sourceMigrations.get(migrationId);
        if (context == null) {
            return CompletableFuture.completedFuture(
                    new MigrationResponse(migrationId, false, "Unknown migration"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Transfer the final delta log
                StateMigrationService.TransferResult result =
                        stateTransferService.transferFinalDelta(
                                migrationId,
                                message.getPartitionId());

                // Notify coordinator
                if (coordinatorCallback != null) {
                    FinalDeltaComplete notification = new FinalDeltaComplete(
                            migrationId,
                            message.getPartitionId(),
                            result.getDeltaSize());

                    coordinatorCallback.sendToCoordinator(notification);
                }

                LOG.info("Final delta transferred for migration {}", migrationId);
                return new MigrationResponse(migrationId, true, "Final delta transferred");

            } catch (Exception e) {
                LOG.error("Failed to transfer final delta for migration {}", migrationId, e);
                return new MigrationResponse(migrationId, false,
                        "Failed to transfer final delta: " + e.getMessage());
            }
        }, executor);
    }

    /**
     * Handle commit migration message.
     */
    public CompletableFuture<MigrationResponse> handleCommitMigration(CommitMigration message) {
        long migrationId = message.getMigrationId();
        LOG.info("Committing migration {}", migrationId);

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Handle on source side
                SourceMigrationContext sourceCtx = sourceMigrations.get(migrationId);
                if (sourceCtx != null) {
                    // Delete migrated partition state
                    stateTransferService.deletePartitionState(
                            migrationId,
                            message.getPartitionId());

                    sourceCtx.setState(MigrationState.COMMITTED);
                    sourceMigrations.remove(migrationId);
                    LOG.info("Source committed for migration {}", migrationId);
                }

                // Handle on target side
                TargetMigrationContext targetCtx = targetMigrations.get(migrationId);
                if (targetCtx != null) {
                    // Buffered post-barrier records are replayed at the operator level (FugueCutover).
                    targetCtx.setState(MigrationState.COMMITTED);
                    targetMigrations.remove(migrationId);
                    LOG.info("Target committed for migration {}", migrationId);
                }

                return new MigrationResponse(migrationId, true, "Migration committed");

            } catch (Exception e) {
                LOG.error("Failed to commit migration {}", migrationId, e);
                return new MigrationResponse(migrationId, false,
                        "Failed to commit: " + e.getMessage());
            }
        }, executor);
    }

    /**
     * Handle abort migration message.
     */
    public CompletableFuture<MigrationResponse> handleAbortMigration(AbortMigration message) {
        long migrationId = message.getMigrationId();
        LOG.warn("Aborting migration {}: {}", migrationId, message.getReason());

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Cleanup source migration
                SourceMigrationContext sourceCtx = sourceMigrations.remove(migrationId);
                if (sourceCtx != null) {
                    sourceCtx.setState(MigrationState.ABORTED);
                    stateTransferService.abortSource(migrationId);
                }

                // Cleanup target migration
                TargetMigrationContext targetCtx = targetMigrations.remove(migrationId);
                if (targetCtx != null) {
                    targetCtx.setState(MigrationState.ABORTED);
                    stateTransferService.abortTarget(migrationId);
                }

                return new MigrationResponse(migrationId, true, "Migration aborted");

            } catch (Exception e) {
                LOG.error("Failed to abort migration {}", migrationId, e);
                return new MigrationResponse(migrationId, false,
                        "Failed to abort: " + e.getMessage());
            }
        }, executor);
    }

    /**
     * Find source migration by partition ID.
     */
    private long findSourceMigration(int partitionId) {
        for (Map.Entry<Long, SourceMigrationContext> entry : sourceMigrations.entrySet()) {
            if (entry.getValue().getPlan().getPartitionId() == partitionId) {
                return entry.getKey();
            }
        }
        return -1;
    }

    /**
     * Find target migration by partition ID.
     */
    private long findTargetMigration(int partitionId) {
        for (Map.Entry<Long, TargetMigrationContext> entry : targetMigrations.entrySet()) {
            if (entry.getValue().getPlan().getPartitionId() == partitionId) {
                return entry.getKey();
            }
        }
        return -1;
    }

    /**
     * Set coordinator callback.
     */
    public void setCoordinatorCallback(CoordinatorCallback callback) {
        this.coordinatorCallback = callback;
    }

    /**
     * Shutdown the controller.
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        stateTransferService.shutdown();
    }

    /**
     * Callback interface for communication with coordinator.
     */
    public interface CoordinatorCallback {
        void sendToCoordinator(MigrationMessage message);
    }

    /**
     * Context for source-side migration.
     */
    private static class SourceMigrationContext {
        private final long migrationId;
        private final MigrationPlan plan;
        private final String targetAddress;
        private final int targetPort;
        private volatile MigrationState state;
        private int roundNumber;
        private long bytesTransferred;
        private long currentDeltaSize;

        public SourceMigrationContext(long migrationId, MigrationPlan plan,
                                     String targetAddress, int targetPort) {
            this.migrationId = migrationId;
            this.plan = plan;
            this.targetAddress = targetAddress;
            this.targetPort = targetPort;
            this.state = MigrationState.INIT;
            this.roundNumber = 0;
            this.bytesTransferred = 0;
            this.currentDeltaSize = 0;
        }

        public long getMigrationId() {
            return migrationId;
        }

        public MigrationPlan getPlan() {
            return plan;
        }

        public MigrationState getState() {
            return state;
        }

        public void setState(MigrationState state) {
            this.state = state;
        }

        public void incrementRound() {
            this.roundNumber++;
        }

        public void addBytesTransferred(long bytes) {
            this.bytesTransferred += bytes;
        }

        public void setCurrentDeltaSize(long size) {
            this.currentDeltaSize = size;
        }
    }

    /**
     * Context for target-side migration.
     */
    private static class TargetMigrationContext {
        private final long migrationId;
        private final MigrationPlan plan;
        private final int listenPort;
        private volatile MigrationState state;
        private volatile boolean barrierReceived;

        public TargetMigrationContext(long migrationId, MigrationPlan plan, int listenPort) {
            this.migrationId = migrationId;
            this.plan = plan;
            this.listenPort = listenPort;
            this.state = MigrationState.INIT;
            this.barrierReceived = false;
        }

        public long getMigrationId() {
            return migrationId;
        }

        public MigrationPlan getPlan() {
            return plan;
        }

        public MigrationState getState() {
            return state;
        }

        public void setState(MigrationState state) {
            this.state = state;
        }

        public void setBarrierReceived(boolean received) {
            this.barrierReceived = received;
        }
    }

    /**
     * The default state-transfer mechanism when none is injected: explicitly <em>unconfigured</em>. The
     * control plane (RPC dispatch, lifecycle, abort/cleanup) is fully functional, but the state plane
     * must be supplied at the operator level — an operator-backed {@code OperatorStateMigrationService}
     * or {@code NetworkedStateMigrationService} — via the 2-arg constructor (see
     * {@code docs/fugue-multinode-deployment.md}). We fail loudly on a transfer attempt rather than
     * silently "moving" disconnected state.
     */
    private static final class UnconfiguredStateMigration implements StateMigrationService {
        @Override
        public void prepareSource(long migrationId, int partitionId, String targetAddress, int targetPort) {
            throw unconfigured();
        }

        @Override
        public void prepareTarget(long migrationId, int partitionId, int listenPort) {
            throw unconfigured();
        }

        @Override
        public StateMigrationService.TransferResult transferSnapshot(
                long migrationId, int partitionId, int roundNumber, long rateLimitBytesPerSec) {
            throw unconfigured();
        }

        @Override
        public StateMigrationService.TransferResult transferFinalDelta(long migrationId, int partitionId) {
            throw unconfigured();
        }

        @Override
        public void stopDeltaLogging(long migrationId) {}

        @Override
        public void deletePartitionState(long migrationId, int partitionId) {}

        @Override
        public void abortSource(long migrationId) {}

        @Override
        public void abortTarget(long migrationId) {}

        @Override
        public void shutdown() {}

        private static IllegalStateException unconfigured() {
            return new IllegalStateException(
                    "No StateMigrationService configured for this MigrationController. The control "
                            + "plane (RPC, lifecycle) is wired, but the state-transfer mechanism must "
                            + "be supplied at the operator level — an operator-backed "
                            + "OperatorStateMigrationService or NetworkedStateMigrationService — via "
                            + "the 2-arg MigrationController constructor (see "
                            + "docs/fugue-multinode-deployment.md).");
        }
    }
}