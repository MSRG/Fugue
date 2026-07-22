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
import org.rocksdb.RocksIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-key-group <em>delta capture under live writes</em>. On a MiniCluster, while a
 * "hot" key in the migrating key-group is being continuously updated, Fugue does an initial SST bulk
 * copy then iterative snapshot-diff delta rounds; after writes quiesce a final
 * round converges and the target key-group equals ground truth byte-for-byte (the with-delta case,
 * with a controlled final delta in place of the atomic cutover).
 */
class OperatorStateDeltaTransferITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";
    private static final String HOT = "key-7";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    static CountDownLatch SOURCE_EMITTED;
    static final AtomicInteger PROCESSED = new AtomicInteger();
    static final AtomicInteger HOT_EMITTED = new AtomicInteger();
    static final AtomicInteger HOT_PROCESSED = new AtomicInteger();
    static volatile boolean UPDATING;

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void deltaCaptureConvergesUnderLiveWrites() throws Exception {
        FugueBackendRegistry.clear();
        PROCESSED.set(0);
        HOT_EMITTED.set(0);
        HOT_PROCESSED.set(0);
        UPDATING = true;
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

        final JobClient job = env.executeAsync("fugue-phase2-delta-capture");
        try {
            assertTrue(SOURCE_EMITTED.await(30, TimeUnit.SECONDS), "source did not finish initial emit");
            awaitCondition(
                    () -> PROCESSED.get() >= keys.size(),
                    Duration.ofSeconds(30),
                    "initial keyed state not built");
            awaitCondition(
                    () -> FugueBackendRegistry.subtasks().size() >= 2,
                    Duration.ofSeconds(30),
                    "fewer than 2 operator backends registered");

            final int kg = KeyGroupRangeAssignment.assignToKeyGroup(HOT, MAX_PARALLELISM);
            final int sourceSubtask =
                    KeyGroupRangeAssignment.assignKeyToParallelOperator(HOT, MAX_PARALLELISM, SLOTS);
            Integer targetSubtask = null;
            for (Integer st : FugueBackendRegistry.subtasks()) {
                if (st != sourceSubtask) {
                    targetSubtask = st;
                    break;
                }
            }
            assertNotNull(targetSubtask, "no distinct target subtask");

            final RocksDBKeyedStateBackend<?> sBackend = FugueBackendRegistry.get(sourceSubtask);
            final RocksDBKeyedStateBackend<?> tBackend = FugueBackendRegistry.get(targetSubtask);
            assertNotNull(sBackend, "source backend (owner of the hot key) not registered");

            final int prefix = RocksDBBackendAccessor.keyGroupPrefixBytes(sBackend);
            final RocksDB sDb = RocksDBBackendAccessor.db(sBackend);
            final RocksDB tDb = RocksDBBackendAccessor.db(tBackend);
            final ColumnFamilyHandle sCf = RocksDBBackendAccessor.columnFamily(sBackend, STATE_NAME);
            final ColumnFamilyHandle tCf = RocksDBBackendAccessor.columnFamily(tBackend, STATE_NAME);

            final Map<String, String> targetBefore = dumpAll(tDb, tCf);

            final OperatorStateMigrationService svc =
                    new OperatorStateMigrationService(sBackend, tBackend, List.of(STATE_NAME));
            svc.prepareSource(1L, kg, "ignored", 0);
            svc.prepareTarget(1L, kg, 0);

            // Round 1: SST bulk copy. Capture the key-group snapshot the target holds right after bulk.
            svc.transferSnapshot(1L, kg, 1, Long.MAX_VALUE);
            final Map<String, String> kgAfterBulk = dumpKeyGroup(tDb, tCf, kg, prefix);
            assertFalse(kgAfterBulk.isEmpty(), "bulk copied nothing for the migrating key-group");

            // Rounds 2-4: iterative delta while the hot key is being written concurrently.
            for (int round = 2; round <= 4; round++) {
                svc.transferSnapshot(1L, kg, round, Long.MAX_VALUE);
                Thread.sleep(100);
            }

            // Quiesce writes to the migrating key-group, then drain in-flight hot records.
            UPDATING = false;
            awaitCondition(
                    () -> HOT_PROCESSED.get() == HOT_EMITTED.get(),
                    Duration.ofSeconds(30),
                    "hot-key writes did not drain");

            // Final delta rounds until converged (deltaSize == 0).
            long delta;
            int round = 5;
            final int maxRound = 25;
            do {
                delta = svc.transferSnapshot(1L, kg, round++, Long.MAX_VALUE).getDeltaSize();
            } while (delta != 0 && round < maxRound);
            assertEquals(0L, delta, "delta did not converge to 0 after writes quiesced");

            // (1) Target key-group equals source ground truth, byte-for-byte.
            assertEquals(
                    dumpKeyGroup(sDb, sCf, kg, prefix),
                    dumpKeyGroup(tDb, tCf, kg, prefix),
                    "target key-group != source after convergence");
            // (2) Non-trivial: the source key-group actually changed under live writes since the bulk copy
            //     (so the equality above was achieved by the deltas, not just the bulk).
            assertNotEquals(
                    kgAfterBulk,
                    dumpKeyGroup(sDb, sCf, kg, prefix),
                    "source key-group did not change under live writes; delta path not exercised");
            // (3) Target's own key-groups are untouched by the migration.
            final Map<String, String> targetAfter = dumpAll(tDb, tCf);
            for (Map.Entry<String, String> e : targetBefore.entrySet()) {
                assertEquals(e.getValue(), targetAfter.get(e.getKey()), "target's own entry changed");
            }

            svc.shutdown();
        } finally {
            job.cancel();
        }
    }

    /** Running count into ValueState "count"; registers the state in open() (so the CF exists on every
     *  subtask) and tracks how many hot-key records it has processed (for drain synchronization). */
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
            if (HOT.equals(key)) {
                HOT_PROCESSED.incrementAndGet();
            }
            out.collect(Tuple2.of(key, next));
        }
    }

    /** Emits every key once, then keeps re-emitting the hot key while {@code UPDATING} (live writes to
     *  the migrating key-group), staying alive until cancelled. */
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
                if (HOT.equals(k)) {
                    HOT_EMITTED.incrementAndGet();
                }
            }
            SOURCE_EMITTED.countDown();
            while (running) {
                if (UPDATING) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(HOT);
                    }
                    HOT_EMITTED.incrementAndGet();
                }
                Thread.sleep(20);
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

    private static Map<String, String> dumpAll(RocksDB db, ColumnFamilyHandle cf) {
        final Map<String, String> out = new LinkedHashMap<>();
        try (RocksIterator it = db.newIterator(cf)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.put(hex(it.key()), hex(it.value()));
            }
        }
        return out;
    }

    private static Map<String, String> dumpKeyGroup(
            RocksDB db, ColumnFamilyHandle cf, int keyGroup, int prefixBytes) {
        final Map<String, String> out = new LinkedHashMap<>();
        for (KeyGroupStateTransfer.Entry e :
                KeyGroupStateTransfer.extractKeyGroup(db, cf, keyGroup, prefixBytes)) {
            out.put(hex(e.key), hex(e.value));
        }
        return out;
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
