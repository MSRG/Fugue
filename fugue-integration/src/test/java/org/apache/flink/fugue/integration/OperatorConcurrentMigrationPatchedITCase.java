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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.fugue.runtime.barrier.FugueStreamTaskRegistry;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueCutover;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.OperatorStateMigrationService;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: <b>concurrent multi-partition migration</b> — a single
 * {@link MigrationBarrier} carrying a list of independent migration plans atomically cuts over several
 * key-groups at once. Here two key-groups (from two different source subtasks) migrate to the <b>same</b>
 * target B in one barrier, which also exercises the per-(subtask,key-group) generalization of
 * {@link FugueCutover}/{@link FugueKeyedStateOperator} (B buffers + hosts two migrating key-groups
 * simultaneously). Distinct-target concurrent migration is a strictly simpler subset of this.
 *
 * <p>Asserts both migrations are correct (each migrated key's count == its independently-known
 * total), record integrity, and that non-migrating key-groups keep flowing throughout.
 *
 * <p>Tagged {@code "patched"}; run via {@code mvn -o verify -P patched}.
 */
@Tag("patched")
class OperatorConcurrentMigrationPatchedITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";
    private static final String SOURCE_NAME = "fugue-source";
    private static final int N1 = 4;
    private static final int N2 = 4;
    private static final int N3 = 4;

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
    static CountDownLatch GATE3;
    static CountDownLatch P3_DONE;

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void twoKeyGroupsMigrateConcurrentlyInOneBarrier() throws Exception {
        LAST_COUNT.clear();
        FugueBackendRegistry.clear();
        FugueRoutingOverrides.clear();
        FugueStreamTaskRegistry.clear();
        FugueCutover.clear();
        P1_DONE = new CountDownLatch(1);
        GATE2 = new CountDownLatch(1);
        P2_DONE = new CountDownLatch(1);
        GATE3 = new CountDownLatch(1);
        P3_DONE = new CountDownLatch(1);

        // Two migrating keys, each alone in its key-group, owned by two DIFFERENT source subtasks.
        final Map<Integer, Integer> kgPopulation = new java.util.HashMap<>();
        final Map<String, Integer> kgOfKey = new java.util.HashMap<>();
        for (int i = 0; i < 200; i++) {
            final String key = "key-" + i;
            final int g = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
            kgOfKey.put(key, g);
            kgPopulation.merge(g, 1, Integer::sum);
        }
        String k1 = null;
        int o1 = -1;
        String k2 = null;
        int o2 = -1;
        for (int i = 0; i < 200; i++) {
            final String key = "key-" + i;
            if (kgPopulation.get(kgOfKey.get(key)) != 1) {
                continue; // need a singleton key-group
            }
            final int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, MAX_PARALLELISM, SLOTS);
            if (k1 == null) {
                k1 = key;
                o1 = owner;
            } else if (owner != o1) { // second migrating key must have a different owner
                k2 = key;
                o2 = owner;
                break;
            }
        }
        assertTrue(k2 != null, "need two singleton-key-group keys with different owners");
        final String mk1 = k1;
        final String mk2 = k2;
        final int a1 = o1;
        final int a2 = o2;
        final int g1 = kgOfKey.get(mk1);
        final int g2 = kgOfKey.get(mk2);
        // Target B: a subtask that owns neither g1 nor g2 (so both are cross-range for B).
        int target = -1;
        for (int s = 0; s < SLOTS; s++) {
            if (s != a1 && s != a2) {
                target = s;
                break;
            }
        }
        final int subtaskB = target;
        assertTrue(subtaskB >= 0, "need a target distinct from both source subtasks");

        final List<String> background = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final String key = "key-" + i;
            if (!key.equals(mk1) && !key.equals(mk2)) {
                background.add(key);
            }
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new ThreePhaseSource(background, Arrays.asList(mk1, mk2)))
                .setParallelism(1)
                .name(SOURCE_NAME)
                .returns(String.class)
                .keyBy(k -> k)
                .transform("fugue-keyed", org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase4-concurrent-migration");
        try {
            assertTrue(P1_DONE.await(30, TimeUnit.SECONDS), "phase 1 not emitted");
            awaitCondition(() -> lastCount(a1, mk1) == N1 && lastCount(a2, mk2) == N1, Duration.ofSeconds(30),
                    "sources did not process all phase-1 records");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 3, Duration.ofSeconds(30),
                    "operator backends not registered");

            final RocksDBKeyedStateBackend<String> bA1 = backend(a1);
            final RocksDBKeyedStateBackend<String> bA2 = backend(a2);
            final RocksDBKeyedStateBackend<String> bB = backend(subtaskB);

            // Pre-copy both key-groups A1->B and A2->B; host both on B; arm B to buffer both.
            final OperatorStateMigrationService svc1 =
                    new OperatorStateMigrationService(bA1, bB, List.of(STATE_NAME));
            final OperatorStateMigrationService svc2 =
                    new OperatorStateMigrationService(bA2, bB, List.of(STATE_NAME));
            svc1.transferSnapshot(1L, g1, 1, Long.MAX_VALUE);
            svc2.transferSnapshot(2L, g2, 1, Long.MAX_VALUE);
            bB.addHostedKeyGroup(g1);
            bB.addHostedKeyGroup(g2);
            FugueCutover.armBuffering(subtaskB, g1);
            FugueCutover.armBuffering(subtaskB, g2);

            // ONE barrier carrying BOTH migration plans -> atomic concurrent flip.
            final MigrationPlan plan1 =
                    new MigrationPlan(g1, new OperatorID(),
                            new MigrationPlan.OperatorInstance(a1, "tm-" + a1),
                            new MigrationPlan.OperatorInstance(subtaskB, "tm-" + subtaskB),
                            new JobID(), 0L);
            final MigrationPlan plan2 =
                    new MigrationPlan(g2, new OperatorID(),
                            new MigrationPlan.OperatorInstance(a2, "tm-" + a2),
                            new MigrationPlan.OperatorInstance(subtaskB, "tm-" + subtaskB),
                            new JobID(), 0L);
            FugueStreamTaskRegistry.injectInto(
                    SOURCE_NAME, MigrationBarrier.createStandalone(1L, Arrays.asList(plan1, plan2)));
            awaitCondition(
                    () -> Integer.valueOf(subtaskB).equals(FugueRoutingOverrides.getOverride(g1))
                            && Integer.valueOf(subtaskB).equals(FugueRoutingOverrides.getOverride(g2)),
                    Duration.ofSeconds(30), "both routing flips not installed");

            GATE2.countDown();
            assertTrue(P2_DONE.await(30, TimeUnit.SECONDS), "phase 2 not emitted");

            assertEquals(N1, lastCount(a1, mk1), "source A1 advanced past the cut");
            assertEquals(N1, lastCount(a2, mk2), "source A2 advanced past the cut");

            svc1.transferSnapshot(1L, g1, 2, Long.MAX_VALUE);
            svc2.transferSnapshot(2L, g2, 2, Long.MAX_VALUE);
            FugueCutover.requestReplay(subtaskB, g1);
            FugueCutover.requestReplay(subtaskB, g2);

            GATE3.countDown();
            assertTrue(P3_DONE.await(30, TimeUnit.SECONDS), "phase 3 not emitted");
            awaitCondition(
                    () -> lastCount(subtaskB, mk1) == N1 + N2 + N3 && lastCount(subtaskB, mk2) == N1 + N2 + N3,
                    Duration.ofSeconds(30), "B did not reach the expected counts for both migrated keys");

            // Both migrations are correct + record integrity.
            assertEquals(N1 + N2 + N3, lastCount(subtaskB, mk1), "mk1 migrated count");
            assertEquals(N1 + N2 + N3, lastCount(subtaskB, mk2), "mk2 migrated count");
            assertEquals(N1, lastCount(a1, mk1), "A1 stopped at the cut");
            assertEquals(N1, lastCount(a2, mk2), "A2 stopped at the cut");
            assertEquals(Long.valueOf(N1 + N2 + N3), readCount(bB, mk1), "B state for mk1 == ground truth");
            assertEquals(Long.valueOf(N1 + N2 + N3), readCount(bB, mk2), "B state for mk2 == ground truth");

            // Non-migrating key-groups never blocked: each background key fully processed by its owner.
            for (String bg : background) {
                final int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(bg, MAX_PARALLELISM, SLOTS);
                awaitCondition(() -> lastCount(owner, bg) == 3, Duration.ofSeconds(30),
                        "background key " + bg + " not fully processed");
                assertEquals(3L, lastCount(owner, bg), "background key " + bg);
            }

            svc1.shutdown();
            svc2.shutdown();
        } finally {
            job.cancel();
            FugueCutover.clear();
            FugueRoutingOverrides.clear();
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
        desc.initializeSerializerUnlessSet(new org.apache.flink.api.common.ExecutionConfig());
        final ValueState<Long> state =
                backend.getPartitionedState(
                        org.apache.flink.runtime.state.VoidNamespace.INSTANCE,
                        org.apache.flink.runtime.state.VoidNamespaceSerializer.INSTANCE,
                        desc);
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

    /** Each phase: every background key once + each migrating key N times (N1/N2/N3). */
    static class ThreePhaseSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private final List<String> background;
        private final List<String> migratingKeys;
        private volatile boolean running = true;

        ThreePhaseSource(List<String> background, List<String> migratingKeys) {
            this.background = background;
            this.migratingKeys = migratingKeys;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            emit(ctx, N1);
            P1_DONE.countDown();
            GATE2.await();
            emit(ctx, N2);
            P2_DONE.countDown();
            GATE3.await();
            emit(ctx, N3);
            P3_DONE.countDown();
            while (running) {
                Thread.sleep(50);
            }
        }

        private void emit(SourceContext<String> ctx, int mkTimes) {
            synchronized (ctx.getCheckpointLock()) {
                for (String bg : background) {
                    ctx.collect(bg);
                }
                for (String mk : migratingKeys) {
                    for (int i = 0; i < mkTimes; i++) {
                        ctx.collect(mk);
                    }
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
