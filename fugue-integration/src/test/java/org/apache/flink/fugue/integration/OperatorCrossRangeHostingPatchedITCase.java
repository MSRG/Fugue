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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.OperatorStateMigrationService;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: <b>cross-range hosting</b>. A Flink keyed operator subtask is statically responsible for
 * a contiguous {@link KeyGroupRange}
 * and its keyed state backend rejects any key-group outside it (the guard in
 * {@code InternalKeyContextImpl#setCurrentKeyGroupIndex}). For an online migration, the <em>target</em>
 * instance {@code O_new} must serve the migrated key-group even though it lies outside that static range
 * (atomic cutover).
 *
 * <p>Patch {@code 07-cross-range-key-group-hosting} relaxes that one guard with a mutable hosted-key-group
 * set, exposed as {@code AbstractKeyedStateBackend#addHostedKeyGroup}. This test proves, on a real patched
 * MiniCluster, that after (1) transferring a key-group's state A→B via the
 * {@link OperatorStateMigrationService} and (2) adding it to B's hosted set, B can address the key-group
 * (previously an {@code IllegalArgumentException}) and <em>serves the migrated value through the normal
 * keyed-state API</em>, byte-equivalent to what the source instance serves (for the hosted
 * key-group, no routing flip yet).
 *
 * <p>Tagged {@code "patched"} (needs the {@code build-flink.sh} runtime); run via
 * {@code mvn -o verify -P patched}. On clean (unpatched) Flink the target rejects the foreign
 * key-group, so this hosting behaviour exists only on the patched fork.
 */
@Tag("patched")
class OperatorCrossRangeHostingPatchedITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    static CountDownLatch SOURCE_EMITTED;
    static final AtomicInteger PROCESSED = new AtomicInteger();

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void targetServesKeyGroupOutsideItsStaticRange() throws Exception {
        FugueBackendRegistry.clear();
        PROCESSED.set(0);
        SOURCE_EMITTED = new CountDownLatch(1);

        // Build the input stream: each distinct key is emitted a distinctive number of times, so its
        // running count is a non-trivial, key-specific ground truth (not all 1s).
        final List<String> distinctKeys = new ArrayList<>();
        final Map<String, Long> expectedCount = new HashMap<>();
        final List<String> emissions = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final String key = "key-" + i;
            final long times = 1 + (i % 7);
            distinctKeys.add(key);
            expectedCount.put(key, times);
            for (long t = 0; t < times; t++) {
                emissions.add(key);
            }
        }
        final int totalRecords = emissions.size();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new GatedSource(emissions))
                .setParallelism(1)
                .returns(String.class)
                .keyBy(k -> k)
                .transform(
                        "fugue-keyed",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase3-cross-range-hosting");
        try {
            assertTrue(SOURCE_EMITTED.await(30, TimeUnit.SECONDS), "source did not finish emitting");
            awaitCondition(
                    () -> PROCESSED.get() >= totalRecords,
                    Duration.ofSeconds(30),
                    "operators did not process all records");
            awaitCondition(
                    () -> FugueBackendRegistry.subtasks().size() >= 2,
                    Duration.ofSeconds(30),
                    "fewer than 2 operator backends registered");

            // Group keys by owning subtask; pick a source S (with data) and a different target T.
            final Map<Integer, List<String>> bySubtask = new HashMap<>();
            for (String k : distinctKeys) {
                bySubtask
                        .computeIfAbsent(
                                KeyGroupRangeAssignment.assignKeyToParallelOperator(k, MAX_PARALLELISM, SLOTS),
                                x -> new ArrayList<>())
                        .add(k);
            }
            Integer sourceSubtask = null;
            Integer targetSubtask = null;
            for (Integer st : FugueBackendRegistry.subtasks()) {
                if (!bySubtask.containsKey(st)) {
                    continue;
                }
                if (sourceSubtask == null) {
                    sourceSubtask = st;
                } else {
                    targetSubtask = st;
                    break;
                }
            }
            assertNotNull(sourceSubtask, "no populated source subtask");
            assertNotNull(targetSubtask, "no distinct target subtask");

            final String migratingKey = bySubtask.get(sourceSubtask).get(0);
            final int kg = KeyGroupRangeAssignment.assignToKeyGroup(migratingKey, MAX_PARALLELISM);
            final long expected = expectedCount.get(migratingKey);

            final RocksDBKeyedStateBackend<String> sBackend = backend(sourceSubtask);
            final RocksDBKeyedStateBackend<String> tBackend = backend(targetSubtask);

            // Sanity: the migrating key-group is genuinely outside the target's static range.
            final KeyGroupRange targetRange =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                            MAX_PARALLELISM, SLOTS, targetSubtask);
            assertFalse(
                    targetRange.contains(kg),
                    "test precondition: kg " + kg + " must be outside target range " + targetRange);

            // (1) Before hosting, the guard rejects the foreign key-group on the target.
            assertThrows(
                    IllegalArgumentException.class,
                    () -> tBackend.setCurrentKey(migratingKey),
                    "target must reject a key-group outside its static range before hosting");

            // (2) Transfer the key-group's state A→B (SST bulk path) and host it on B.
            final OperatorStateMigrationService svc =
                    new OperatorStateMigrationService(sBackend, tBackend, List.of(STATE_NAME));
            svc.prepareSource(1L, kg, "ignored", 0);
            svc.prepareTarget(1L, kg, 0);
            svc.transferSnapshot(1L, kg, 1, Long.MAX_VALUE);
            tBackend.addHostedKeyGroup(kg);

            // (3) The target now addresses the hosted key-group...
            tBackend.setCurrentKey(migratingKey);
            assertEquals(kg, tBackend.getCurrentKeyGroupIndex(), "current key-group should be the hosted kg");

            // (4) ...and serves the migrated value through the normal keyed-state API, equal to ground
            // truth and to what the source instance serves (for the hosted key-group).
            assertEquals(expected, readCount(sBackend, migratingKey), "source value sanity");
            assertEquals(expected, readCount(tBackend, migratingKey), "target must serve the migrated value");

            svc.shutdown();
        } finally {
            job.cancel();
        }
    }

    @SuppressWarnings("unchecked")
    private static RocksDBKeyedStateBackend<String> backend(int subtask) {
        return (RocksDBKeyedStateBackend<String>)
                (RocksDBKeyedStateBackend<?>) FugueBackendRegistry.get(subtask);
    }

    /** Read the running-count ValueState for {@code key} through the backend's keyed-state API. */
    private static Long readCount(RocksDBKeyedStateBackend<String> backend, String key) throws Exception {
        backend.setCurrentKey(key);
        final ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>(STATE_NAME, Long.class);
        desc.initializeSerializerUnlessSet(new ExecutionConfig());
        final ValueState<Long> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);
        return state.value();
    }

    /** Keyed running count into ValueState "count"; registers the state in {@code open()}. */
    static class RunningCount extends KeyedProcessFunction<String, String, Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>(STATE_NAME, Long.class));
        }

        @Override
        public void processElement(String key, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            final Long current = count.value();
            final long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            PROCESSED.incrementAndGet();
            out.collect(Tuple2.of(key, next));
        }
    }

    /** Emits the given record stream (parallelism 1), then stays alive (no further writes) until cancelled. */
    static class GatedSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private final List<String> emissions;
        private volatile boolean running = true;

        GatedSource(List<String> emissions) {
            this.emissions = emissions;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (String k : emissions) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(k);
                }
            }
            SOURCE_EMITTED.countDown();
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
