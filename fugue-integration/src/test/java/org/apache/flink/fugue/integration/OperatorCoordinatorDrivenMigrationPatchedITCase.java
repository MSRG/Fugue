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

package org.apache.flink.fugue.integration;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;
import org.apache.flink.fugue.coordinator.rpc.FugueCoordinatorEndpoint;
import org.apache.flink.fugue.integration.taskmanager.FugueTaskExecutorService;
import org.apache.flink.fugue.runtime.barrier.FugueBarrierAligner;
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.fugue.runtime.barrier.FugueStreamTaskRegistry;
import org.apache.flink.fugue.runtime.controller.MigrationController;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueCutover;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.OperatorStateMigrationService;
import org.apache.flink.fugue.runtime.transfer.StateMigrationService;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.RocksDB;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: <b>coordinator-driven</b> online migration over real RPC. A real
 * {@link FugueCoordinatorEndpoint} (its {@link org.apache.flink.fugue.coordinator.manager.MigrationManager}
 * state machine) drives a migration end-to-end over a real {@link TestingRpcService} against a real
 * {@link FugueTaskExecutorService} controller, on a <em>live</em> MiniCluster job — the coordinator, not
 * the test, sequences prepare → pre-copy → barrier → finalize → commit. The barrier is really injected by
 * the coordinator (into the live sources); the controller executes the real cutover (host + pre-copy +
 * buffer + final delta + replay) via an operator-backed {@link StateMigrationService}.
 *
 * <p>Single-TaskManager setup: the coordinator/controller endpoints are
 * created in the test over a real {@code RpcService} (rather than embedded in JobMaster/TaskExecutor), and
 * the one controller plays both source and target roles (one TaskManager). The RPC, the protocol, the
 * barrier injection, and the cutover are all exercised here; multi-TaskManager address resolution from the
 * {@code ExecutionGraph} and cross-process network state transfer are handled by the cluster wiring described
 * in the README.
 *
 * <p>Tagged {@code "patched"}; run via {@code mvn -o verify -P patched}.
 */
@Tag("patched")
class OperatorCoordinatorDrivenMigrationPatchedITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";
    private static final String SOURCE_NAME = "fugue-source";
    private static final String OP_NAME = "fugue-keyed";
    private static final int MK_TOTAL = 30;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    static final Map<Integer, Map<String, Long>> LAST_COUNT = new ConcurrentHashMap<>();
    static CountDownLatch MK_DONE;

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void coordinatorDrivesOnlineMigrationOverRpc() throws Exception {
        LAST_COUNT.clear();
        FugueBackendRegistry.clear();
        FugueRoutingOverrides.clear();
        FugueStreamTaskRegistry.clear();
        FugueCutover.clear();
        FugueBarrierAligner.clear();
        MK_DONE = new CountDownLatch(1);

        final String mk = "key-7";
        final int kg = KeyGroupRangeAssignment.assignToKeyGroup(mk, MAX_PARALLELISM);
        final int subtaskA = KeyGroupRangeAssignment.assignKeyToParallelOperator(mk, MAX_PARALLELISM, SLOTS);
        final int subtaskB = (subtaskA + 1) % SLOTS;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new MkThenBackgroundSource(mk))
                .setParallelism(1)
                .name(SOURCE_NAME)
                .returns(String.class)
                .keyBy(k -> k)
                .transform(OP_NAME, org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase4-coordinator-driven");
        final TestingRpcService rpc = new TestingRpcService();
        FugueCoordinatorEndpoint coordinator = null;
        FugueTaskExecutorService controller = null;
        try {
            awaitCondition(() -> lastCount(subtaskA, mk) >= 3, Duration.ofSeconds(30),
                    "A did not start processing MK");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 2, Duration.ofSeconds(30),
                    "operator backends not registered");

            final JobID jobId = new JobID();
            final RocksDBKeyedStateBackend<String> bA = backend(subtaskA);
            final RocksDBKeyedStateBackend<String> bB = backend(subtaskB);

            // The controller's state-transfer mechanism: a real operator-backed cutover (host + pre-copy +
            // arm-buffer + final delta + replay) wrapping the OperatorStateMigrationService.
            final OperatorCutover cutover =
                    new OperatorCutover(bA, bB, subtaskB, kg, Collections.singletonList(STATE_NAME));

            coordinator = new FugueCoordinatorEndpoint(rpc, jobId, new PolicyConfiguration());
            coordinator.start();
            controller =
                    new FugueTaskExecutorService(
                            rpc, "tm-0", new MigrationController("tm-0", cutover));
            controller.start();
            controller.setCoordinatorAddress(coordinator.getAddress());

            // The coordinator really injects the barrier into the live sources, then drives the cutover
            // forward once the barrier has aligned across the operator subtasks.
            final FugueCoordinatorEndpoint coord = coordinator;
            coordinator.getMigrationManager().setBarrierCallback(barrier -> {
                FugueStreamTaskRegistry.injectInto(SOURCE_NAME, barrier);
                final long migId = barrier.getMigrationId();
                final Thread waiter =
                        new Thread(() -> {
                            try {
                                awaitCondition(() -> operatorSubtasksAligned(migId), Duration.ofSeconds(30),
                                        "barrier did not align");
                                coord.notifyBarrierAligned(migId);
                            } catch (Throwable t) {
                                // surfaced via the migration timing out -> abort
                            }
                        }, "fugue-test-barrier-align-waiter");
                waiter.setDaemon(true);
                waiter.start();
            });

            // The plan: migrate kg from A to B; both roles live on the single controller (one TM).
            final String tm = controller.getAddress();
            final MigrationPlan plan =
                    new MigrationPlan(
                            kg,
                            new OperatorID(),
                            new MigrationPlan.OperatorInstance(subtaskA, tm),
                            new MigrationPlan.OperatorInstance(subtaskB, tm),
                            jobId,
                            0L);

            // --- Trigger the coordinator; it drives the whole protocol over RPC. ---
            final List<Long> ids =
                    coordinator.startMigrations(Collections.singletonList(plan)).get(15, TimeUnit.SECONDS);
            assertEquals(1, ids.size());
            final long migrationId = ids.get(0);
            final CompletableFuture<MigrationState> completion =
                    coordinator.getMigrationManager().getCompletion(migrationId);

            final MigrationState terminal = completion.get(60, TimeUnit.SECONDS);
            assertEquals(MigrationState.COMMITTED, terminal, "coordinator must drive the migration to COMMITTED");

            // After the coordinator-driven migration, B owns kg and serves the correct value: every MK
            // record is counted exactly once across the A→B handoff.
            assertTrue(MK_DONE.await(30, TimeUnit.SECONDS), "source did not finish emitting MK");
            awaitCondition(() -> lastCount(subtaskB, mk) == MK_TOTAL, Duration.ofSeconds(30),
                    "B did not converge to the migrated MK total (got " + lastCount(subtaskB, mk) + ")");
            assertEquals(MK_TOTAL, lastCount(subtaskB, mk), "B serves MK's full count");
            assertEquals(Long.valueOf(MK_TOTAL), readCount(bB, mk), "B's persisted state for MK == ground truth");
            assertTrue(lastCount(subtaskA, mk) <= MK_TOTAL, "A handed off at the cut");
        } finally {
            if (controller != null) {
                controller.closeAsync().get(10, TimeUnit.SECONDS);
            }
            if (coordinator != null) {
                coordinator.closeAsync().get(10, TimeUnit.SECONDS);
            }
            rpc.closeAsync().get(10, TimeUnit.SECONDS);
            FugueRoutingOverrides.clear();
            FugueCutover.clear();
            job.cancel();
        }
    }

    /**
     * True once all operator subtasks are aligned for {@code migId}. Waiting for every subtask (the
     * barrier is broadcast to all) ensures the source instance A has drained all pre-barrier records
     * before the coordinator ships the final delta.
     */
    private static boolean operatorSubtasksAligned(long migId) {
        int aligned = 0;
        for (String task : FugueBarrierAligner.tasks(migId)) {
            if (!task.contains(OP_NAME)) {
                continue;
            }
            final FugueBarrierAligner.AlignmentState s = FugueBarrierAligner.state(task, migId);
            if (s != null && s.isAligned()) {
                aligned++;
            }
        }
        return aligned >= SLOTS;
    }

    /** Operator-backed cutover plugged into the controller as its StateMigrationService. */
    private static final class OperatorCutover implements StateMigrationService {
        private final int targetSubtask;
        private final int keyGroup;
        private final OperatorStateMigrationService transfer;
        private final RocksDBKeyedStateBackend<String> target;

        OperatorCutover(
                RocksDBKeyedStateBackend<String> source,
                RocksDBKeyedStateBackend<String> target,
                int targetSubtask,
                int keyGroup,
                List<String> stateNames)
                throws Exception {
            this.target = target;
            this.targetSubtask = targetSubtask;
            this.keyGroup = keyGroup;
            this.transfer = new OperatorStateMigrationService(source, target, stateNames);
        }

        @Override
        public void prepareSource(long migrationId, int partitionId, String targetAddress, int targetPort) {}

        @Override
        public void prepareTarget(long migrationId, int partitionId, int listenPort) {
            // O_new: host the migrating key-group and start buffering its post-flip records.
            target.addHostedKeyGroup(keyGroup);
            FugueCutover.armBuffering(targetSubtask, keyGroup);
        }

        @Override
        public StateMigrationService.TransferResult transferSnapshot(
                long migrationId, int partitionId, int roundNumber, long rateLimitBytesPerSec)
                throws Exception {
            // Background bulk copy; converges in one round here (low-churn key-group).
            return transfer.transferSnapshot(migrationId, keyGroup, roundNumber, rateLimitBytesPerSec);
        }

        @Override
        public StateMigrationService.TransferResult transferFinalDelta(long migrationId, int partitionId)
                throws Exception {
            // Final delta makes B == A at the cut, then release the buffered post-flip records.
            final StateMigrationService.TransferResult r =
                    transfer.transferSnapshot(migrationId, keyGroup, 2, Long.MAX_VALUE);
            FugueCutover.requestReplay(targetSubtask, keyGroup);
            return r;
        }

        @Override
        public void stopDeltaLogging(long migrationId) {}

        @Override
        public void deletePartitionState(long migrationId, int partitionId) {}

        @Override
        public void abortSource(long migrationId) {
            transfer.abortSource(migrationId);
        }

        @Override
        public void abortTarget(long migrationId) {
            transfer.abortTarget(migrationId);
            FugueRoutingOverrides.clearOverride(keyGroup);
            FugueCutover.finishAll(targetSubtask);
        }

        @Override
        public void shutdown() {
            transfer.shutdown();
        }
    }

    private static long lastCount(int subtask, String key) {
        final Map<String, Long> m = LAST_COUNT.get(subtask);
        if (m == null) {
            return 0L;
        }
        final Long v = m.get(key);
        return v == null ? 0L : v;
    }

    @SuppressWarnings("unchecked")
    private static RocksDBKeyedStateBackend<String> backend(int subtask) {
        return (RocksDBKeyedStateBackend<String>)
                (RocksDBKeyedStateBackend<?>) FugueBackendRegistry.get(subtask);
    }

    private static Long readCount(RocksDBKeyedStateBackend<String> backend, String key) throws Exception {
        backend.setCurrentKey(key);
        final ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>(STATE_NAME, Long.class);
        desc.initializeSerializerUnlessSet(new ExecutionConfig());
        final ValueState<Long> state =
                backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);
        return state.value();
    }

    static class RunningCount extends KeyedProcessFunction<String, String, String> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>(STATE_NAME, Long.class));
        }

        @Override
        public void processElement(String key, Context ctx, Collector<String> out) throws Exception {
            final Long current = count.value();
            final long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            LAST_COUNT.computeIfAbsent(getRuntimeContext().getIndexOfThisSubtask(), s -> new ConcurrentHashMap<>())
                    .put(key, next);
            out.collect(key);
        }
    }

    /** Emits the migrating key MK exactly MK_TOTAL times (spread over time), with continuous background. */
    static class MkThenBackgroundSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private final String mk;
        private volatile boolean running = true;

        MkThenBackgroundSource(String mk) {
            this.mk = mk;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < MK_TOTAL && running; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(mk);
                    for (int b = 0; b < 6; b++) {
                        ctx.collect("bg-" + b);
                    }
                }
                Thread.sleep(150);
            }
            MK_DONE.countDown();
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int b = 0; b < 6; b++) {
                        ctx.collect("bg-" + b);
                    }
                }
                Thread.sleep(150);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private interface Condition {
        boolean met();
    }

    private static void awaitCondition(Condition c, Duration timeout, String message)
            throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (c.met()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("timed out: " + message);
    }
}
