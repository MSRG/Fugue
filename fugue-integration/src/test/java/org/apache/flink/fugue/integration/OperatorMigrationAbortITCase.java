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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBBackendAccessor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.KeyGroupStateTransfer;
import org.apache.flink.fugue.runtime.transfer.OperatorStateMigrationService;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Migration <b>abort / rollback</b>. When a migration is aborted
 * pre-emptively (a transient failure during pre-copy exhausts the coordinator's retry-with-timeout),
 * {@code O_new} deletes any partially received state and {@code O_old} retains ownership; the query
 * continues in its stable, pre-migration configuration. This runs on a live MiniCluster:
 *
 * <ol>
 *   <li>the source A processes the migrating key-group (phase 1);
 *   <li>the key-group is pre-copied A→B (so B holds a partial snapshot);
 *   <li>the migration is aborted — {@link OperatorStateMigrationService#abortTarget} discards B's
 *       partially-received state;
 *   <li>processing continues (phase 2): the routing was never flipped, so the key-group stays with A,
 *       which keeps counting correctly.
 * </ol>
 *
 * Asserts the pre-copy actually populated B, that abort fully discarded it, that no routing flip ever
 * occurred, and that A's state for the key reflects <em>every</em> record (no loss, no duplication) —
 * i.e. the abort left the system exactly as if no migration had been attempted.
 *
 * <p>No Flink patch is needed (no cross-range hosting or routing flip happens before a pre-copy abort),
 * so this runs in the clean build.
 */
class OperatorMigrationAbortITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";
    private static final int N1 = 6;
    private static final int N2 = 6;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    static final Map<Integer, Map<String, Long>> LAST_COUNT = new ConcurrentHashMap<>();
    static CountDownLatch P1_DONE;
    static CountDownLatch GATE2;
    static CountDownLatch P2_DONE;

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void abortDiscardsTargetStateAndSourceKeepsOwnership() throws Exception {
        LAST_COUNT.clear();
        FugueBackendRegistry.clear();
        FugueRoutingOverrides.clear();
        P1_DONE = new CountDownLatch(1);
        GATE2 = new CountDownLatch(1);
        P2_DONE = new CountDownLatch(1);

        final String mk = "key-7";
        final int kg = KeyGroupRangeAssignment.assignToKeyGroup(mk, MAX_PARALLELISM);
        final int subtaskA = KeyGroupRangeAssignment.assignKeyToParallelOperator(mk, MAX_PARALLELISM, SLOTS);
        final int subtaskB = (subtaskA + 1) % SLOTS;

        final List<String> background = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            final String key = "bg-" + i;
            if (!key.equals(mk)) {
                background.add(key);
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new TwoPhaseSource(background, mk))
                .setParallelism(1)
                .name("fugue-source")
                .returns(String.class)
                .keyBy(k -> k)
                .transform("fugue-keyed", org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase4-abort");
        try {
            assertTrue(P1_DONE.await(30, TimeUnit.SECONDS), "phase 1 not emitted");
            awaitCondition(() -> lastCount(subtaskA, mk) == N1, Duration.ofSeconds(30),
                    "A did not process all phase-1 records for MK");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 2, Duration.ofSeconds(30),
                    "operator backends not registered");

            final RocksDBKeyedStateBackend<String> bA = backend(subtaskA);
            final RocksDBKeyedStateBackend<String> bB = backend(subtaskB);

            // Pre-copy A->B: B gets a partial snapshot of the key-group.
            final OperatorStateMigrationService svc =
                    new OperatorStateMigrationService(bA, bB, List.of(STATE_NAME));
            svc.transferSnapshot(1L, kg, 1, Long.MAX_VALUE);
            assertFalse(keyGroupEntries(bB, kg).isEmpty(), "pre-copy should have populated B's key-group");

            // --- Abort: O_new discards its partially-received state; O_old retains ownership. ---
            svc.abortSource(1L);
            svc.abortTarget(1L);
            assertTrue(keyGroupEntries(bB, kg).isEmpty(), "abort must discard B's partially-received state");

            // Emission continues; routing was never flipped, so MK stays with A.
            GATE2.countDown();
            assertTrue(P2_DONE.await(30, TimeUnit.SECONDS), "phase 2 not emitted");
            awaitCondition(() -> lastCount(subtaskA, mk) == N1 + N2, Duration.ofSeconds(30),
                    "A did not keep processing MK after the abort");

            // No routing flip ever happened; A owns + counted every record; B holds nothing.
            assertNull(FugueRoutingOverrides.getOverride(kg), "routing must never have flipped");
            assertFalse(FugueRoutingOverrides.isActive(), "no routing override should be active");
            assertEquals(N1 + N2, lastCount(subtaskA, mk), "A must have counted every MK record (no loss/dup)");
            assertEquals(0L, lastCount(subtaskB, mk), "B must never have processed MK");
            assertTrue(keyGroupEntries(bB, kg).isEmpty(), "B's key-group remains empty after abort");

            svc.shutdown();
        } finally {
            job.cancel();
        }
    }

    private static List<KeyGroupStateTransfer.Entry> keyGroupEntries(
            RocksDBKeyedStateBackend<String> backend, int kg) {
        final int prefix = RocksDBBackendAccessor.keyGroupPrefixBytes(backend);
        final ColumnFamilyHandle cf = RocksDBBackendAccessor.columnFamily(backend, STATE_NAME);
        return KeyGroupStateTransfer.extractKeyGroup(RocksDBBackendAccessor.db(backend), cf, kg, prefix);
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

    static class TwoPhaseSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private final List<String> background;
        private final String mk;
        private volatile boolean running = true;

        TwoPhaseSource(List<String> background, String mk) {
            this.background = background;
            this.mk = mk;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            emit(ctx, N1);
            P1_DONE.countDown();
            GATE2.await();
            emit(ctx, N2);
            P2_DONE.countDown();
            while (running) {
                Thread.sleep(50);
            }
        }

        private void emit(SourceContext<String> ctx, int mkTimes) {
            synchronized (ctx.getCheckpointLock()) {
                for (String bg : background) {
                    ctx.collect(bg);
                }
                for (int i = 0; i < mkTimes; i++) {
                    ctx.collect(mk);
                }
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
