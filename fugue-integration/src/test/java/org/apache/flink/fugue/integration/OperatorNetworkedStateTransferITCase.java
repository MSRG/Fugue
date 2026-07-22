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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBBackendAccessor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.fugue.runtime.transfer.KeyGroupStateTransfer;
import org.apache.flink.fugue.runtime.transfer.NetworkedKeyGroupTransfer;
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
import org.rocksdb.RocksIterator;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-key-group state transfer over a real <b>TCP socket</b>
 * ({@link NetworkedKeyGroupTransfer}) — the cross-TaskManager replacement for the in-JVM
 * backend→backend copy. On a MiniCluster, one key-group is streamed from a source operator backend to a
 * target operator backend over a loopback socket and asserted byte-for-byte identical, with the target's
 * own key-groups untouched. The socket I/O is real (loopback is real network I/O); only the two
 * endpoints being co-located distinguishes this from a two-node run.
 *
 * <p>No Flink patch needed (clean build).
 */
class OperatorNetworkedStateTransferITCase {

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
    void keyGroupStreamsOverTcpByteForByte() throws Exception {
        FugueBackendRegistry.clear();
        PROCESSED.set(0);
        SOURCE_EMITTED = new CountDownLatch(1);

        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("key-" + i);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new GatedSource(keys))
                .setParallelism(1)
                .returns(String.class)
                .keyBy(k -> k)
                .transform(
                        "fugue-keyed",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-networked-state-transfer");
        try {
            assertTrue(SOURCE_EMITTED.await(30, TimeUnit.SECONDS), "source did not finish emitting");
            awaitCondition(() -> PROCESSED.get() >= keys.size(), Duration.ofSeconds(30), "all records");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 2, Duration.ofSeconds(30),
                    "≥2 operator backends");

            final Map<Integer, List<String>> bySubtask = new HashMap<>();
            for (String k : keys) {
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

            final int kg =
                    KeyGroupRangeAssignment.assignToKeyGroup(bySubtask.get(sourceSubtask).get(0), MAX_PARALLELISM);
            final RocksDBKeyedStateBackend<?> sBackend = FugueBackendRegistry.get(sourceSubtask);
            final RocksDBKeyedStateBackend<?> tBackend = FugueBackendRegistry.get(targetSubtask);
            final int prefix = RocksDBBackendAccessor.keyGroupPrefixBytes(sBackend);

            final List<KeyGroupStateTransfer.Entry> sourceKg =
                    KeyGroupStateTransfer.extractKeyGroup(
                            RocksDBBackendAccessor.db(sBackend),
                            RocksDBBackendAccessor.columnFamily(sBackend, STATE_NAME),
                            kg,
                            prefix);
            assertFalse(sourceKg.isEmpty(), "source key-group has no state to transfer");
            final Map<String, String> targetBefore = dump(tBackend);

            // --- Stream the key-group over a real loopback TCP socket: target listens, source connects. ---
            final ServerSocket server = NetworkedKeyGroupTransfer.listen(0);
            final int port = server.getLocalPort();
            final AtomicReference<Throwable> receiverError = new AtomicReference<>();
            final Thread receiver =
                    new Thread(
                            () -> {
                                try {
                                    NetworkedKeyGroupTransfer.receiveAndIngest(server, tBackend);
                                } catch (Throwable t) {
                                    receiverError.set(t);
                                }
                            },
                            "fugue-test-receiver");
            receiver.start();
            final long sent =
                    NetworkedKeyGroupTransfer.send(sBackend, List.of(STATE_NAME), kg, "localhost", port, true);
            receiver.join(30_000);
            server.close();
            if (receiverError.get() != null) {
                throw new AssertionError("receiver failed", receiverError.get());
            }
            assertTrue(sent > 0, "should have streamed some bytes");

            // (1) target gained the key-group byte-for-byte; (2) source intact; (3) target's own state untouched.
            assertEntriesEqual(
                    sourceKg,
                    KeyGroupStateTransfer.extractKeyGroup(
                            RocksDBBackendAccessor.db(tBackend),
                            RocksDBBackendAccessor.columnFamily(tBackend, STATE_NAME),
                            kg,
                            prefix));
            assertEntriesEqual(
                    sourceKg,
                    KeyGroupStateTransfer.extractKeyGroup(
                            RocksDBBackendAccessor.db(sBackend),
                            RocksDBBackendAccessor.columnFamily(sBackend, STATE_NAME),
                            kg,
                            prefix));
            final Map<String, String> targetAfter = dump(tBackend);
            final Map<String, String> migrated = new HashMap<>();
            for (KeyGroupStateTransfer.Entry e : sourceKg) {
                migrated.put(hex(e.key), hex(e.value));
            }
            for (Map.Entry<String, String> e : targetBefore.entrySet()) {
                assertEquals(e.getValue(), targetAfter.get(e.getKey()), "target's own entry changed");
            }
            for (Map.Entry<String, String> e : targetAfter.entrySet()) {
                if (!targetBefore.containsKey(e.getKey())) {
                    assertTrue(migrated.containsKey(e.getKey()), "unexpected new key in target: " + e.getKey());
                }
            }
        } finally {
            job.cancel();
        }
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
        private final List<String> keys;
        private volatile boolean running = true;

        GatedSource(List<String> keys) {
            this.keys = keys;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (String k : keys) {
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

    private static Map<String, String> dump(RocksDBKeyedStateBackend<?> backend) {
        final Map<String, String> out = new LinkedHashMap<>();
        final ColumnFamilyHandle cf = RocksDBBackendAccessor.columnFamily(backend, STATE_NAME);
        try (RocksIterator it = RocksDBBackendAccessor.db(backend).newIterator(cf)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.put(hex(it.key()), hex(it.value()));
            }
        }
        return out;
    }

    private static void assertEntriesEqual(
            List<KeyGroupStateTransfer.Entry> a, List<KeyGroupStateTransfer.Entry> b) {
        final Map<String, String> ma = new HashMap<>();
        final Map<String, String> mb = new HashMap<>();
        for (KeyGroupStateTransfer.Entry e : a) {
            ma.put(hex(e.key), hex(e.value));
        }
        for (KeyGroupStateTransfer.Entry e : b) {
            mb.put(hex(e.key), hex(e.value));
        }
        assertEquals(ma, mb, "key-group entries differ");
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
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
