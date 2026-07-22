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

package org.apache.flink.fugue.examples;

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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A runnable example that runs a keyed RocksDB job whose stateful operator is
 * a {@link FugueKeyedStateOperator}, then migrates one key-group's live state from its source subtask to
 * a target subtask using the per-key-group transfer ({@link OperatorStateMigrationService}),
 * and verifies the moved state is byte-for-byte identical (the no-traffic case).
 *
 * <p>This runs on an embedded MiniCluster against <em>stock</em> Flink 1.18 — it needs no patched fork,
 * because the per-key-group state transfer reaches the live backend through the same-package
 * {@code RocksDBBackendAccessor} (no reflection, no Flink-core patch).
 *
 * <p>The <b>full online cutover</b> — under continuous traffic, with the in-band routing flip,
 * cross-range hosting of the migrated key-group at the target, post-barrier buffering/replay, and the
 * coordinator driving it over RPC — requires the Fugue-patched Flink fork and is demonstrated (and
 * asserted, incl. record integrity and "non-migrating partitions never block") by the {@code @Tag(
 * "patched")} integration tests: build the fork with {@code ./build-flink.sh} and run
 * {@code mvn -o verify -P patched} (see {@code OperatorOnlineMigrationPatchedITCase} and
 * {@code OperatorCoordinatorDrivenMigrationPatchedITCase}).
 *
 * <p>Run with JDK 17.
 */
public final class FugueMigrationExample {

    private static final int PARALLELISM = 4;
    private static final int MAX_PARALLELISM = 128;
    private static final String STATE_NAME = "count";

    private static final AtomicInteger PROCESSED = new AtomicInteger();

    private FugueMigrationExample() {}

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        FugueBackendRegistry.clear();

        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("key-" + i);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.addSource(new FiniteSource(keys))
                .setParallelism(1)
                .returns(String.class)
                .keyBy(k -> k)
                .transform(
                        "fugue-keyed",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(PARALLELISM)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-migration-example");
        try {
            awaitUntil(() -> PROCESSED.get() >= keys.size(), 30_000, "operators to process all records");
            awaitUntil(() -> FugueBackendRegistry.subtasks().size() >= 2, 30_000, "≥2 operator backends");

            // Find a source subtask that owns some keys, and a distinct target subtask.
            final Map<Integer, List<String>> bySubtask = new HashMap<>();
            for (String k : keys) {
                bySubtask
                        .computeIfAbsent(
                                KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                        k, MAX_PARALLELISM, PARALLELISM),
                                x -> new ArrayList<>())
                        .add(k);
            }
            Integer source = null;
            Integer target = null;
            for (Integer st : FugueBackendRegistry.subtasks()) {
                if (!bySubtask.containsKey(st)) {
                    continue;
                }
                if (source == null) {
                    source = st;
                } else {
                    target = st;
                    break;
                }
            }
            if (source == null || target == null) {
                System.out.println("Could not find a source/target subtask pair; try rerunning.");
                return;
            }

            final String migratingKey = bySubtask.get(source).get(0);
            final int kg = KeyGroupRangeAssignment.assignToKeyGroup(migratingKey, MAX_PARALLELISM);

            final RocksDBKeyedStateBackend<?> sBackend = FugueBackendRegistry.get(source);
            final RocksDBKeyedStateBackend<?> tBackend = FugueBackendRegistry.get(target);
            final int prefix = RocksDBBackendAccessor.keyGroupPrefixBytes(sBackend);

            final int before =
                    KeyGroupStateTransfer.extractKeyGroup(
                                    RocksDBBackendAccessor.db(sBackend),
                                    RocksDBBackendAccessor.columnFamily(sBackend, STATE_NAME),
                                    kg,
                                    prefix)
                            .size();

            System.out.printf(
                    "Migrating key-group %d (e.g. key '%s') from subtask %d -> subtask %d ...%n",
                    kg, migratingKey, source, target);

            final OperatorStateMigrationService svc =
                    new OperatorStateMigrationService(sBackend, tBackend, List.of(STATE_NAME));
            svc.transferSnapshot(1L, kg, 1, Long.MAX_VALUE);

            final List<KeyGroupStateTransfer.Entry> src =
                    KeyGroupStateTransfer.extractKeyGroup(
                            RocksDBBackendAccessor.db(sBackend),
                            RocksDBBackendAccessor.columnFamily(sBackend, STATE_NAME),
                            kg,
                            prefix);
            final List<KeyGroupStateTransfer.Entry> tgt =
                    KeyGroupStateTransfer.extractKeyGroup(
                            RocksDBBackendAccessor.db(tBackend),
                            RocksDBBackendAccessor.columnFamily(tBackend, STATE_NAME),
                            kg,
                            prefix);

            final boolean identical = sameEntries(src, tgt);
            System.out.printf(
                    "Transferred %d key-group-%d entries to subtask %d; byte-for-byte identical: %b%n",
                    before, kg, target, identical);
            System.out.println(
                    "Full online cutover (under traffic, routing flip + cross-range hosting + buffering, "
                            + "coordinator-driven over RPC) is demonstrated by the @Tag(\"patched\") ITs: "
                            + "./build-flink.sh <flink-1.18.0> && mvn -o verify -P patched");

            svc.shutdown();
        } finally {
            job.cancel();
        }
    }

    private static boolean sameEntries(
            List<KeyGroupStateTransfer.Entry> a, List<KeyGroupStateTransfer.Entry> b) {
        final Map<String, String> ma = new HashMap<>();
        final Map<String, String> mb = new HashMap<>();
        for (KeyGroupStateTransfer.Entry e : a) {
            ma.put(hex(e.key), hex(e.value));
        }
        for (KeyGroupStateTransfer.Entry e : b) {
            mb.put(hex(e.key), hex(e.value));
        }
        return ma.equals(mb);
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private interface Cond {
        boolean met();
    }

    private static void awaitUntil(Cond c, long timeoutMs, String what) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (c.met()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("timed out waiting for " + what);
    }

    /** Emits each key once, then stays alive until cancelled. */
    static final class FiniteSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private final List<String> keys;
        private volatile boolean running = true;

        FiniteSource(List<String> keys) {
            this.keys = keys;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (String k : keys) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(k);
                }
            }
            while (running) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Keyed running count into ValueState "count". */
    static final class RunningCount extends KeyedProcessFunction<String, String, Tuple2<String, Long>> {
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
}
