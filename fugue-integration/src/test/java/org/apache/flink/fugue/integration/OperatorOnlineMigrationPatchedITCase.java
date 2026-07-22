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
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.fugue.runtime.barrier.FugueStreamTaskRegistry;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueCutover;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.OperatorStateMigrationService;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: the full <b>online A→B key-group migration under live
 * traffic</b>. Composes every piece: pre-copy (SST bulk), the
 * in-band routing flip, cross-range hosting at the target, target-side post-barrier buffering
 * ({@link FugueKeyedStateOperator} + {@link FugueCutover}), the final delta, and replay.
 *
 * <p>A keyed running-count job runs continuously. The migrating key MK (alone in its key-group) is
 * migrated from its owner A to a target B while records keep flowing:
 *
 * <ol>
 *   <li>phase 1 records for MK are processed by A (count → N1);
 *   <li>the coordinator (here the test) pre-copies MK's key-group A→B, hosts it on B, arms B's buffer,
 *       and injects the barrier that flips routing MK→B in-band;
 *   <li>phase 2 records for MK now arrive at B and are <em>held</em> (B's state is not yet final);
 *   <li>A has quiesced for MK at the cut; the final delta is shipped A→B;
 *   <li>replay is signalled — B drains its held records in order on top of the transferred state, then
 *       processes phase 3 live.
 * </ol>
 *
 * <p>Asserts <b>state correctness</b> (B's count for MK == N1+N2+N3, the independently-known total, i.e. exactly
 * the value with no migration), <b>record integrity</b> (every MK record counted exactly once: A stopped
 * at N1, B carried it to the total — no loss, no duplication), and that <b>non-migrating partitions were
 * never blocked</b> (every other key was fully processed by its single owner throughout the cutover).
 *
 * <p>The cutover <em>orchestration</em> (deciding when to pre-copy / flip / ship the final delta
 * / replay) is driven by the test in place of the coordinator, as are the barrier-injection and
 * backend discovery. The mechanisms exercised on real patched Flink
 * are the in-band flip, cross-range hosting, operator-level buffering + replay, and per-key-group
 * state transfer. Checkpoint/recovery of a hosted key-group is out of scope.
 *
 * <p>Tagged {@code "patched"}; run via {@code mvn -o verify -P patched}.
 */
@Tag("patched")
class OperatorOnlineMigrationPatchedITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";
    private static final String SOURCE_NAME = "fugue-source";
    private static final int N1 = 5;
    private static final int N2 = 5;
    private static final int N3 = 4;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    /** subtask → key → latest running count emitted (== the keyed state value at that point). */
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
    void onlineMigrationPreservesStateAndRecordIntegrity() throws Exception {
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

        // Migrating key MK alone in its key-group; A is its static owner, B a distinct target.
        final Map<Integer, Integer> kgPopulation = new HashMap<>();
        final Map<String, Integer> kgOfKey = new HashMap<>();
        for (int i = 0; i < 120; i++) {
            final String key = "key-" + i;
            final int g = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
            kgOfKey.put(key, g);
            kgPopulation.merge(g, 1, Integer::sum);
        }
        String chosen = null;
        for (int i = 0; i < 120; i++) {
            final String key = "key-" + i;
            if (kgPopulation.get(kgOfKey.get(key)) == 1) {
                chosen = key;
                break;
            }
        }
        assertTrue(chosen != null, "need a key alone in its key-group");
        final String mk = chosen;
        final int kg = kgOfKey.get(mk);
        final int subtaskA = KeyGroupRangeAssignment.assignKeyToParallelOperator(mk, MAX_PARALLELISM, SLOTS);
        final int subtaskB = (subtaskA + 1) % SLOTS;
        assertNotEquals(subtaskA, subtaskB);

        final List<String> backgroundKeys = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            final String key = "key-" + i;
            if (!key.equals(mk)) {
                backgroundKeys.add(key);
            }
        }

        // Each phase: every background key once + MK n times. Background keys (other key-groups) witness
        // that non-migrating partitions keep flowing throughout the cutover.
        final List<String> phase1 = phase(backgroundKeys, mk, N1);
        final List<String> phase2 = phase(backgroundKeys, mk, N2);
        final List<String> phase3 = phase(backgroundKeys, mk, N3);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new ThreePhaseSource(phase1, phase2, phase3))
                .setParallelism(1)
                .name(SOURCE_NAME)
                .returns(String.class)
                .keyBy(k -> k)
                .transform("fugue-keyed", org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase3-online-migration");
        try {
            assertTrue(P1_DONE.await(30, TimeUnit.SECONDS), "phase 1 not emitted");
            awaitCondition(() -> lastCount(subtaskA, mk) == N1, Duration.ofSeconds(30),
                    "A did not process all phase-1 records for MK");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 2, Duration.ofSeconds(30),
                    "fewer than 2 backends registered");

            final KeyGroupRange rangeB =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(MAX_PARALLELISM, SLOTS, subtaskB);
            assertFalse(rangeB.contains(kg), "kg " + kg + " must be outside target range " + rangeB);

            final RocksDBKeyedStateBackend<String> sBackend = backend(subtaskA);
            final RocksDBKeyedStateBackend<String> tBackend = backend(subtaskB);

            // (2) Pre-copy MK's key-group A->B (SST bulk), host it on B, arm B's buffer.
            final OperatorStateMigrationService svc =
                    new OperatorStateMigrationService(sBackend, tBackend, List.of(STATE_NAME));
            svc.prepareSource(1L, kg, "ignored", 0);
            svc.prepareTarget(1L, kg, 0);
            svc.transferSnapshot(1L, kg, 1, Long.MAX_VALUE);
            tBackend.addHostedKeyGroup(kg);
            FugueCutover.armBuffering(subtaskB, kg);

            // Flip routing MK -> B, in-band, via the barrier.
            final MigrationPlan plan =
                    new MigrationPlan(
                            kg,
                            new OperatorID(),
                            new MigrationPlan.OperatorInstance(subtaskA, "tm-a"),
                            new MigrationPlan.OperatorInstance(subtaskB, "tm-b"),
                            new JobID(),
                            0L);
            FugueStreamTaskRegistry.injectInto(
                    SOURCE_NAME, MigrationBarrier.createStandalone(1L, Collections.singletonList(plan)));
            awaitCondition(() -> Integer.valueOf(subtaskB).equals(FugueRoutingOverrides.getOverride(kg)),
                    Duration.ofSeconds(30), "routing flip not installed");

            // (3) phase 2 -> B, held in the operator buffer.
            GATE2.countDown();
            assertTrue(P2_DONE.await(30, TimeUnit.SECONDS), "phase 2 not emitted");

            // (4) A has quiesced for MK at the cut (received no phase-2 MK). Ship the final delta A->B.
            assertEquals(N1, lastCount(subtaskA, mk), "A must not have advanced past the cut");
            svc.transferSnapshot(1L, kg, 2, Long.MAX_VALUE);

            // (5) Replay: B drains held records on top of the transferred state, then processes phase 3 live.
            FugueCutover.requestReplay(subtaskB, kg);
            GATE3.countDown();
            assertTrue(P3_DONE.await(30, TimeUnit.SECONDS), "phase 3 not emitted");
            awaitCondition(() -> lastCount(subtaskB, mk) == N1 + N2 + N3, Duration.ofSeconds(30),
                    "B did not reach the expected migrated count for MK");

            // --- State correctness + record integrity ---
            assertEquals(N1 + N2 + N3, lastCount(subtaskB, mk),
                    "B must serve MK's count == total records emitted (ground truth, exactly-once)");
            assertEquals(N1, lastCount(subtaskA, mk), "A stopped at the cut (no double counting)");
            for (int st = 0; st < SLOTS; st++) {
                if (st != subtaskA && st != subtaskB) {
                    assertEquals(0L, lastCount(st, mk), "no other subtask should have processed MK");
                }
            }
            // Rigorous: B's persisted keyed state for MK equals ground truth (operator is now idle).
            assertEquals(Long.valueOf(N1 + N2 + N3), readCount(tBackend, mk),
                    "B's RocksDB-backed state for MK == ground truth");

            // --- Non-migrating partitions never blocked: every other key fully processed by its owner. ---
            for (String bg : backgroundKeys) {
                final int owner = KeyGroupRangeAssignment.assignKeyToParallelOperator(bg, MAX_PARALLELISM, SLOTS);
                awaitCondition(() -> lastCount(owner, bg) == 3, Duration.ofSeconds(30),
                        "background key " + bg + " not fully processed (3 phases)");
                assertEquals(3L, lastCount(owner, bg), "background key " + bg + " count");
                for (int st = 0; st < SLOTS; st++) {
                    if (st != owner) {
                        assertEquals(0L, lastCount(st, bg), "background key " + bg + " leaked to subtask " + st);
                    }
                }
            }

            svc.shutdown();
        } finally {
            job.cancel();
            FugueCutover.clear();
            FugueRoutingOverrides.clear();
        }
    }

    private static List<String> phase(List<String> background, String mk, int mkTimes) {
        final List<String> out = new ArrayList<>(background);
        for (int i = 0; i < mkTimes; i++) {
            out.add(mk);
        }
        return out;
    }

    /** Latest count emitted by {@code subtask} for {@code key}, or 0 if none. */
    private static long lastCount(int subtask, String key) {
        final Map<String, Long> byKey = LAST_COUNT.get(subtask);
        if (byKey == null) {
            return 0L;
        }
        final Long v = byKey.get(key);
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

    /** Keyed running count into ValueState "count"; mirrors the count into LAST_COUNT for observation. */
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

    /** Emits phase 1, then waits for GATE2, emits phase 2, waits for GATE3, emits phase 3, then idles. */
    static class ThreePhaseSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private final List<String> phase1;
        private final List<String> phase2;
        private final List<String> phase3;
        private volatile boolean running = true;

        ThreePhaseSource(List<String> phase1, List<String> phase2, List<String> phase3) {
            this.phase1 = phase1;
            this.phase2 = phase2;
            this.phase3 = phase3;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            emit(ctx, phase1);
            P1_DONE.countDown();
            GATE2.await();
            emit(ctx, phase2);
            P2_DONE.countDown();
            GATE3.await();
            emit(ctx, phase3);
            P3_DONE.countDown();
            while (running) {
                Thread.sleep(50);
            }
        }

        private void emit(SourceContext<String> ctx, List<String> records) {
            for (String r : records) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(r);
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
