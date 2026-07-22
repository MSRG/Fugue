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
import org.apache.flink.contrib.streaming.state.RocksDBBackendAccessor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueCutover;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.KeyGroupStateTransfer;
import org.apache.flink.fugue.runtime.transfer.NetworkedStateMigrationService;
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
import org.rocksdb.ColumnFamilyHandle;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-node (CN1, patched fork): the cross-TaskManager {@link NetworkedStateMigrationService} exercised
 * over a real loopback socket. Two instances (a source bound to subtask A's live backend, a target bound
 * to subtask B's) move a key-group A→B: the target hosts the key-group + arms its buffer + listens; the
 * source streams the key-group over TCP; the final round releases the buffer. Asserts the target hosts +
 * serves the migrated value through the normal keyed-state API (== ground truth) and that the final round
 * triggers replay — i.e. the cluster transfer service works end-to-end over a genuine socket (only the
 * two ends being co-located distinguishes it from a true two-node run). The full operator
 * buffering/replay under traffic is verified separately (F3/P4.4).
 *
 * <p>Tagged {@code "patched"}; run via {@code mvn -o verify -P patched}.
 */
@Tag("patched")
class OperatorNetworkedMigrationServicePatchedITCase {

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
    void networkedServiceMigratesKeyGroupOverSocket() throws Exception {
        FugueBackendRegistry.clear();
        FugueCutover.clear();
        PROCESSED.set(0);
        SOURCE_EMITTED = new CountDownLatch(1);

        final List<String> emissions = new ArrayList<>();
        final Map<String, Long> expected = new HashMap<>();
        for (int i = 0; i < 200; i++) {
            final String key = "key-" + i;
            final long times = 1 + (i % 7);
            expected.put(key, times);
            for (long t = 0; t < times; t++) {
                emissions.add(key);
            }
        }
        final int total = emissions.size();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new GatedSource(emissions))
                .setParallelism(1)
                .returns(String.class)
                .keyBy(k -> k)
                .transform("fugue-keyed", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-networked-migration-service");
        NetworkedStateMigrationService sourceSvc = null;
        NetworkedStateMigrationService targetSvc = null;
        try {
            assertTrue(SOURCE_EMITTED.await(30, TimeUnit.SECONDS), "source did not finish");
            awaitCondition(() -> PROCESSED.get() >= total, Duration.ofSeconds(30), "all records");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 2, Duration.ofSeconds(30), "backends");

            final Map<Integer, List<String>> bySubtask = new HashMap<>();
            for (String k : expected.keySet()) {
                bySubtask
                        .computeIfAbsent(
                                KeyGroupRangeAssignment.assignKeyToParallelOperator(k, MAX_PARALLELISM, SLOTS),
                                x -> new ArrayList<>())
                        .add(k);
            }
            Integer a = null;
            Integer b = null;
            for (Integer st : FugueBackendRegistry.subtasks()) {
                if (!bySubtask.containsKey(st)) {
                    continue;
                }
                if (a == null) {
                    a = st;
                } else {
                    b = st;
                    break;
                }
            }
            assertNotNull(a, "no populated source subtask");
            assertNotNull(b, "no distinct target subtask");
            final int subtaskA = a;
            final int subtaskB = b;
            final String mk = bySubtask.get(subtaskA).get(0);
            final int kg = KeyGroupRangeAssignment.assignToKeyGroup(mk, MAX_PARALLELISM);
            final long expectedCount = expected.get(mk);

            final RocksDBKeyedStateBackend<String> bA = backend(subtaskA);
            final RocksDBKeyedStateBackend<String> bB = backend(subtaskB);

            sourceSvc = new NetworkedStateMigrationService(bA, subtaskA, List.of(STATE_NAME));
            targetSvc = new NetworkedStateMigrationService(bB, subtaskB, List.of(STATE_NAME));

            // Target hosts kg + arms buffer + starts listening; source learns the port.
            targetSvc.prepareTarget(1L, kg, 0);
            final int port = targetSvc.getListenPort();
            assertTrue(port > 0, "target should be listening on a port");
            sourceSvc.prepareSource(1L, kg, "localhost", port);

            // Bulk round over the socket: B receives the key-group.
            sourceSvc.transferSnapshot(1L, kg, 1, Long.MAX_VALUE);
            awaitCondition(
                    () -> !keyGroupEntries(bB, kg).isEmpty(),
                    Duration.ofSeconds(30),
                    "target did not receive the key-group over the socket");

            // (1) B now hosts + serves the migrated key-group's value (== ground truth).
            assertEquals(Long.valueOf(expectedCount), readCount(bB, mk),
                    "B must serve the migrated value after the socket transfer");
            assertEntriesEqual(keyGroupEntries(bA, kg), keyGroupEntries(bB, kg));

            // (2) The final round releases the target's buffer (requestReplay).
            sourceSvc.transferFinalDelta(1L, kg);
            awaitCondition(
                    () -> FugueCutover.replayRequested(subtaskB, kg),
                    Duration.ofSeconds(30),
                    "final round did not trigger replay on the target");
            assertEntriesEqual(keyGroupEntries(bA, kg), keyGroupEntries(bB, kg));
        } finally {
            if (sourceSvc != null) {
                sourceSvc.shutdown();
            }
            if (targetSvc != null) {
                targetSvc.shutdown();
            }
            FugueCutover.clear();
            job.cancel();
        }
    }

    private static List<KeyGroupStateTransfer.Entry> keyGroupEntries(
            RocksDBKeyedStateBackend<String> backend, int kg) {
        final int prefix = RocksDBBackendAccessor.keyGroupPrefixBytes(backend);
        final ColumnFamilyHandle cf = RocksDBBackendAccessor.columnFamily(backend, STATE_NAME);
        return KeyGroupStateTransfer.extractKeyGroup(RocksDBBackendAccessor.db(backend), cf, kg, prefix);
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

    private static void assertEntriesEqual(
            List<KeyGroupStateTransfer.Entry> x, List<KeyGroupStateTransfer.Entry> y) {
        final Map<String, String> mx = new HashMap<>();
        final Map<String, String> my = new HashMap<>();
        for (KeyGroupStateTransfer.Entry e : x) {
            mx.put(hex(e.key), hex(e.value));
        }
        for (KeyGroupStateTransfer.Entry e : y) {
            my.put(hex(e.key), hex(e.value));
        }
        assertEquals(mx, my, "key-group entries differ");
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte v : bytes) {
            sb.append(Character.forDigit((v >> 4) & 0xF, 16)).append(Character.forDigit(v & 0xF, 16));
        }
        return sb.toString();
    }

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
