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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.fugue.runtime.barrier.FugueStreamTaskRegistry;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.JobID;

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
 * Patched fork: the <b>atomic in-band routing flip</b>. A
 * migration barrier carrying a plan {@code kg → B} is injected at the source; the patched {@code
 * StreamTask} injector installs the routing override ({@link FugueRoutingOverrides#applyFromBarrier})
 * on the mailbox thread before broadcasting the barrier, and the patched {@code
 * KeyGroupStreamPartitioner#selectChannel} then routes that key-group's records to B instead of its
 * static owner A.
 *
 * <p>The source emits in two phases gated by the test: phase 1 fully drains to A (default routing);
 * the test then hosts {@code kg} on B (cross-range hosting) and injects the barrier; once the override is
 * installed, phase 2 is released and those records route to B. Asserts a clean, <em>targeted</em>
 * hand-off: every phase-1 record for {@code kg} went to A and every phase-2 record went to B, no
 * record was lost or duplicated, and <em>every other key-group kept its original owner</em> (the flip
 * touches only the migrating key-group).
 *
 * <p>Tagged {@code "patched"}; run via {@code mvn -o verify -P patched}. Builds on cross-range hosting (B must host the
 * foreign key-group to process the rerouted records); end-to-end state correctness under the flip is
 * covered by the full online-migration test.
 */
@Tag("patched")
class OperatorRoutingFlipPatchedITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";
    private static final String SOURCE_NAME = "fugue-source";
    private static final int MK_PER_PHASE = 6;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    /** subtask index → key → list of sequence numbers it processed (the routing observation). */
    static final Map<Integer, Map<String, List<Integer>>> SEEN = new ConcurrentHashMap<>();

    static CountDownLatch PHASE1_EMITTED;
    static CountDownLatch PHASE2_GATE;
    static CountDownLatch PHASE2_EMITTED;

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void barrierFlipReroutesOnlyTheMigratingKeyGroup() throws Exception {
        SEEN.clear();
        FugueBackendRegistry.clear();
        FugueRoutingOverrides.clear();
        FugueStreamTaskRegistry.clear();
        PHASE1_EMITTED = new CountDownLatch(1);
        PHASE2_GATE = new CountDownLatch(1);
        PHASE2_EMITTED = new CountDownLatch(1);

        // Pick a migrating key MK whose key-group is unique among the test keys (so flipping that
        // key-group reroutes exactly MK), its static owner A, and a distinct target B.
        final List<String> background = new ArrayList<>();
        final Map<Integer, Integer> kgPopulation = new HashMap<>();
        final Map<String, Integer> kgOfKey = new HashMap<>();
        for (int i = 0; i < 120; i++) {
            final String key = "key-" + i;
            final int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
            kgOfKey.put(key, kg);
            kgPopulation.merge(kg, 1, Integer::sum);
        }
        String mk = null;
        for (int i = 0; i < 120; i++) {
            final String key = "key-" + i;
            if (kgPopulation.get(kgOfKey.get(key)) == 1) {
                mk = key;
                break;
            }
        }
        assertTrue(mk != null, "need a key in a singleton key-group");
        final String migratingKey = mk;
        final int kg = kgOfKey.get(migratingKey);
        final int subtaskA =
                KeyGroupRangeAssignment.assignKeyToParallelOperator(migratingKey, MAX_PARALLELISM, SLOTS);
        final int subtaskB = (subtaskA + 1) % SLOTS;
        assertNotEquals(subtaskA, subtaskB, "source and target subtasks must differ");
        for (int i = 0; i < 120; i++) {
            final String key = "key-" + i;
            if (!key.equals(migratingKey)) {
                background.add(key);
            }
        }

        // First phase: each background key once (seq 0) + MK x MK_PER_PHASE (seq 0..). Second phase: same with
        // a fresh seq base, so phase-1 and phase-2 MK records are distinguishable.
        final List<Tuple2<String, Integer>> phase1 = new ArrayList<>();
        final List<Tuple2<String, Integer>> phase2 = new ArrayList<>();
        for (String bg : background) {
            phase1.add(Tuple2.of(bg, 0));
            phase2.add(Tuple2.of(bg, 1));
        }
        for (int s = 0; s < MK_PER_PHASE; s++) {
            phase1.add(Tuple2.of(migratingKey, s));
            phase2.add(Tuple2.of(migratingKey, MK_PER_PHASE + s));
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new TwoPhaseSource(phase1, phase2))
                .setParallelism(1)
                .name(SOURCE_NAME)
                .returns(TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(t -> t.f0)
                .transform(
                        "fugue-keyed",
                        TypeInformation.of(String.class),
                        new FugueKeyedStateOperator<>(new RecordingCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase3-routing-flip");
        try {
            assertTrue(PHASE1_EMITTED.await(30, TimeUnit.SECONDS), "phase 1 not emitted");
            awaitCondition(
                    () -> seenCount(subtaskA, migratingKey) == MK_PER_PHASE,
                    Duration.ofSeconds(30),
                    "A did not process all phase-1 records for the migrating key");
            awaitCondition(
                    () -> FugueBackendRegistry.subtasks().size() >= 2,
                    Duration.ofSeconds(30),
                    "fewer than 2 operator backends registered");

            // Precondition: the migrating key-group is genuinely outside the target's static range.
            final KeyGroupRange rangeB =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            MAX_PARALLELISM, SLOTS, subtaskB);
            assertFalse(rangeB.contains(kg), "kg " + kg + " must be outside target range " + rangeB);

            // Default routing: A saw all phase-1 MK records; no one else saw MK.
            assertEquals(MK_PER_PHASE, seenCount(subtaskA, migratingKey), "A should hold phase-1 MK records");
            for (int st = 0; st < SLOTS; st++) {
                if (st != subtaskA) {
                    assertEquals(0, seenCount(st, migratingKey), "only A should have MK before the flip");
                }
            }

            // Cross-range hosting: host the foreign key-group on B so it can process the rerouted records.
            backend(subtaskB).addHostedKeyGroup(kg);

            // Inject the migration barrier carrying plan kg -> B; the patched injector installs the
            // routing override in-band on the source's mailbox thread, then broadcasts the barrier.
            final MigrationPlan plan =
                    new MigrationPlan(
                            kg,
                            new OperatorID(),
                            new MigrationPlan.OperatorInstance(subtaskA, "tm-a"),
                            new MigrationPlan.OperatorInstance(subtaskB, "tm-b"),
                            new JobID(),
                            0L);
            final int injected =
                    FugueStreamTaskRegistry.injectInto(
                            SOURCE_NAME, MigrationBarrier.createStandalone(1L, Collections.singletonList(plan)));
            assertTrue(injected >= 1, "barrier should be injected into the source");
            awaitCondition(
                    () -> Integer.valueOf(subtaskB).equals(FugueRoutingOverrides.getOverride(kg)),
                    Duration.ofSeconds(30),
                    "routing override kg->B was not installed by the barrier");

            // Release phase 2: these records route under the flip.
            PHASE2_GATE.countDown();
            assertTrue(PHASE2_EMITTED.await(30, TimeUnit.SECONDS), "phase 2 not emitted");
            awaitCondition(
                    () -> seenCount(subtaskB, migratingKey) == MK_PER_PHASE,
                    Duration.ofSeconds(30),
                    "B did not receive all phase-2 records for the migrating key after the flip");

            // --- The flip is clean and targeted. ---
            // B got exactly the phase-2 MK records; A got no more (still phase-1 only).
            assertEquals(MK_PER_PHASE, seenCount(subtaskB, migratingKey), "B should hold the phase-2 MK records");
            assertEquals(MK_PER_PHASE, seenCount(subtaskA, migratingKey), "A must not receive MK after the flip");
            assertEquals(
                    seqs(subtaskA, migratingKey).stream().sorted().collect(java.util.stream.Collectors.toList()),
                    rangeList(0, MK_PER_PHASE),
                    "A holds exactly the phase-1 sequence numbers");
            assertEquals(
                    seqs(subtaskB, migratingKey).stream().sorted().collect(java.util.stream.Collectors.toList()),
                    rangeList(MK_PER_PHASE, 2 * MK_PER_PHASE),
                    "B holds exactly the phase-2 sequence numbers (no loss, no duplication)");

            // Targeted: every other key was processed by exactly one subtask (its owner) — never rerouted.
            for (String bg : background) {
                int owners = 0;
                for (int st = 0; st < SLOTS; st++) {
                    if (seenCount(st, bg) > 0) {
                        owners++;
                    }
                }
                assertEquals(1, owners, "non-migrating key " + bg + " should keep a single owner across the flip");
            }
        } finally {
            job.cancel();
            // The routing override is JVM-global; failsafe reuses the JVM across IT classes, so clear
            // it here or it would misroute the migrating key-group's records in later tests.
            FugueRoutingOverrides.clear();
        }
    }

    private static int seenCount(int subtask, String key) {
        return seqs(subtask, key).size();
    }

    private static List<Integer> seqs(int subtask, String key) {
        final Map<String, List<Integer>> byKey = SEEN.get(subtask);
        if (byKey == null) {
            return Collections.emptyList();
        }
        final List<Integer> list = byKey.get(key);
        return list == null ? Collections.emptyList() : new ArrayList<>(list);
    }

    private static List<Integer> rangeList(int fromInclusive, int toExclusive) {
        final List<Integer> out = new ArrayList<>();
        for (int i = fromInclusive; i < toExclusive; i++) {
            out.add(i);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static RocksDBKeyedStateBackend<String> backend(int subtask) {
        return (RocksDBKeyedStateBackend<String>)
                (RocksDBKeyedStateBackend<?>) FugueBackendRegistry.get(subtask);
    }

    /** Records which subtask processed each (key, seq); keeps a real keyed ValueState so it is genuinely keyed. */
    static class RecordingCount extends KeyedProcessFunction<String, Tuple2<String, Integer>, String> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>(STATE_NAME, Long.class));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out)
                throws Exception {
            final int subtask = getRuntimeContext().getIndexOfThisSubtask();
            SEEN.computeIfAbsent(subtask, s -> new ConcurrentHashMap<>())
                    .computeIfAbsent(value.f0, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(value.f1);
            final Long current = count.value();
            count.update((current == null ? 0L : current) + 1L);
            out.collect(value.f0);
        }
    }

    /** Emits phase 1, signals, waits for the test to release the gate, emits phase 2, then idles. */
    static class TwoPhaseSource implements SourceFunction<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
        private final List<Tuple2<String, Integer>> phase1;
        private final List<Tuple2<String, Integer>> phase2;
        private volatile boolean running = true;

        TwoPhaseSource(List<Tuple2<String, Integer>> phase1, List<Tuple2<String, Integer>> phase2) {
            this.phase1 = phase1;
            this.phase2 = phase2;
        }

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            for (Tuple2<String, Integer> t : phase1) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(t);
                }
            }
            PHASE1_EMITTED.countDown();
            PHASE2_GATE.await();
            for (Tuple2<String, Integer> t : phase2) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(t);
                }
            }
            PHASE2_EMITTED.countDown();
            while (running) {
                Thread.sleep(50);
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
