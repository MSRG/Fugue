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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.fugue.runtime.operator.FugueBackendRegistry;
import org.apache.flink.fugue.runtime.operator.FugueCutover;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: <b>global-restart fault tolerance</b>. A task failure during
 * an in-flight migration triggers Flink's native global restart; the system reverts to the last
 * (pre-migration) checkpoint, and Fugue's transient, non-checkpointed migration state must be reset so
 * the recovered job runs the pre-migration topology — it does not act on the stale routing flip / hosted
 * key-group / buffering directive. The reset is performed by {@link FugueKeyedStateOperator}'s
 * {@code initializeState} when {@code context.isRestored()} (hosted key-groups reset naturally with the
 * fresh backends).
 *
 * <p>The job runs with checkpointing on. After checkpoints have completed, the test installs a real
 * in-flight migration of key-group {@code kg} (host on B + routing flip MK→B + arm B's buffer), then
 * induces a one-shot task failure. After recovery it asserts: the routing override and cutover directives
 * are cleared; the migrating key MK is again processed by its static owner A (pre-migration topology) and
 * never by B; i.e. the abandoned migration left no trace.
 *
 * <p>Tagged {@code "patched"}; run via {@code mvn -o verify -P patched}.
 */
@Tag("patched")
class OperatorRestartFaultTolerancePatchedITCase {

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

    static final Map<Integer, Map<String, Long>> LAST_COUNT = new ConcurrentHashMap<>();
    static volatile boolean TRIGGER_FAILURE = false;
    static final AtomicBoolean FAILED = new AtomicBoolean(false);
    static final AtomicInteger CHECKPOINTS = new AtomicInteger(0);

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void failureDuringMigrationRevertsToPreMigrationTopology() throws Exception {
        LAST_COUNT.clear();
        FugueBackendRegistry.clear();
        FugueRoutingOverrides.clear();
        FugueCutover.clear();
        TRIGGER_FAILURE = false;
        FAILED.set(false);
        CHECKPOINTS.set(0);

        final String mk = "key-7";
        final int kg = KeyGroupRangeAssignment.assignToKeyGroup(mk, MAX_PARALLELISM);
        final int subtaskA = KeyGroupRangeAssignment.assignKeyToParallelOperator(mk, MAX_PARALLELISM, SLOTS);
        final int subtaskB = (subtaskA + 1) % SLOTS;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(1)));

        env.addSource(new ContinuousSource(mk))
                .setParallelism(1)
                .name("fugue-source")
                .returns(String.class)
                .keyBy(k -> k)
                .transform("fugue-keyed", org.apache.flink.api.common.typeinfo.TypeInformation.of(String.class),
                        new FugueKeyedStateOperator<>(new RunningCount()))
                .setParallelism(SLOTS)
                .addSink(new DiscardingSink<>());

        final JobClient job = env.executeAsync("fugue-phase4-restart-ft");
        try {
            // Let A process MK and let a couple of checkpoints complete (so recovery restores state).
            awaitCondition(() -> lastCount(subtaskA, mk) > 0, Duration.ofSeconds(30), "A not processing MK");
            awaitCondition(() -> CHECKPOINTS.get() >= 3, Duration.ofSeconds(30), "no checkpoints completed");
            awaitCondition(() -> FugueBackendRegistry.subtasks().size() >= 2, Duration.ofSeconds(30),
                    "operator backends not registered");

            // --- Install a real in-flight migration of kg: host on B + arm B's buffer, THEN flip routing
            // (arm before flip so MK never reaches B in an unbuffered window). ---
            backend(subtaskB).addHostedKeyGroup(kg);
            FugueCutover.armBuffering(subtaskB, kg);
            FugueRoutingOverrides.setOverride(kg, subtaskB);
            assertEquals(Integer.valueOf(subtaskB), FugueRoutingOverrides.getOverride(kg), "migration armed");

            // --- Induce a one-shot task failure -> Flink global restart. ---
            TRIGGER_FAILURE = true;

            // After recovery, the operator's initializeState(isRestored) clears the migration state.
            awaitCondition(() -> FAILED.get(), Duration.ofSeconds(30), "failure was not triggered");
            awaitCondition(
                    () -> FugueRoutingOverrides.getOverride(kg) == null && !FugueCutover.isActive(),
                    Duration.ofSeconds(60),
                    "migration state was not reset on restart");

            // The recovered job runs the pre-migration topology: MK is processed by A again, never by B.
            assertNull(FugueRoutingOverrides.getOverride(kg), "routing flip must be gone after restart");
            assertFalse(FugueCutover.isActive(), "buffering directives must be gone after restart");

            final long aAfterReset = lastCount(subtaskA, mk);
            awaitCondition(() -> lastCount(subtaskA, mk) > aAfterReset, Duration.ofSeconds(30),
                    "A did not resume processing MK after recovery (pre-migration topology)");
            assertEquals(0L, lastCount(subtaskB, mk),
                    "B must never have processed MK — the abandoned migration left no trace");
        } finally {
            job.cancel();
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

    /** Running count; records into LAST_COUNT and throws exactly once when failure is triggered. */
    static class RunningCount extends KeyedProcessFunction<String, String, String> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>(STATE_NAME, Long.class));
        }

        @Override
        public void processElement(String key, Context ctx, Collector<String> out) throws Exception {
            if (TRIGGER_FAILURE && FAILED.compareAndSet(false, true)) {
                throw new RuntimeException("fugue-test: induced task failure during migration");
            }
            final Long current = count.value();
            final long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            LAST_COUNT.computeIfAbsent(getRuntimeContext().getIndexOfThisSubtask(), s -> new ConcurrentHashMap<>())
                    .put(key, next);
            out.collect(key);
        }
    }

    /** Continuously emits a handful of background keys + the migrating key; counts checkpoints. */
    static class ContinuousSource implements SourceFunction<String>, CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private final String mk;
        private volatile boolean running = true;

        ContinuousSource(String mk) {
            this.mk = mk;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    for (int i = 0; i < 8; i++) {
                        ctx.collect("bg-" + i);
                    }
                    ctx.collect(mk);
                }
                Thread.sleep(5);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            CHECKPOINTS.incrementAndGet();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}
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
