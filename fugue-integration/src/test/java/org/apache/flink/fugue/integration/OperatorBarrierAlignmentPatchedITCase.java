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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.runtime.barrier.FugueBarrierAligner;
import org.apache.flink.fugue.runtime.barrier.FugueStreamTaskRegistry;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: migration-barrier <em>alignment</em> at the input gate — the
 * consistent-cut decision of the cutover. On a MiniCluster (patched runtime), the barrier is injected
 * at <b>one</b> of two source subtasks first: every downstream operator subtask then has exactly one
 * of its two input channels carrying the barrier and is <b>not</b> aligned. Injecting into the second
 * source delivers the barrier on the remaining channel, and every subtask becomes <b>aligned</b> — the
 * point at which all pre-barrier records have been seen on every channel.
 *
 * <p>This verifies the alignment decision wired into the real input path
 * ({@code CheckpointedInputGate#handleEvent} → {@code FugueBarrierAligner}). Physically holding
 * post-barrier records (true blocking) is coupled to Flink's checkpoint-barrier subpartition machinery
 * and is deferred to the cutover finale (see {@code FugueBarrierAligner} scope note).
 *
 * <p>Tagged {@code "patched"} (needs the {@code build-flink.sh} runtime); run via
 * {@code mvn -o verify -P patched}.
 */
@Tag("patched")
class OperatorBarrierAlignmentPatchedITCase {

    private static final int SOURCE_PARALLELISM = 2;
    private static final int OP_PARALLELISM = 4;
    private static final long MIGRATION_ID = 1L;
    private static final String SOURCE_NAME = "fugue-source";
    private static final String OP_NAME = "fugue-keyed";

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(OP_PARALLELISM)
                            .build());

    static CountDownLatch SOURCE_EMITTED;

    @Test
    void barrierAlignsAcrossAllInputChannels() throws Exception {
        FugueBarrierAligner.clear();
        FugueStreamTaskRegistry.clear();
        SOURCE_EMITTED = new CountDownLatch(SOURCE_PARALLELISM);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(OP_PARALLELISM);

        env.addSource(new GatedSource())
                .setParallelism(SOURCE_PARALLELISM)
                .name(SOURCE_NAME)
                .returns(String.class)
                .keyBy(k -> k)
                .process(new PassThrough())
                .name(OP_NAME)
                .setParallelism(OP_PARALLELISM)
                .addSink(new DiscardingSink<>())
                .setParallelism(1);

        final JobClient job = env.executeAsync("fugue-phase3-barrier-alignment");
        try {
            assertTrue(SOURCE_EMITTED.await(30, TimeUnit.SECONDS), "sources did not start");
            awaitCondition(
                    () -> countTasks(SOURCE_NAME) == SOURCE_PARALLELISM,
                    Duration.ofSeconds(30),
                    "source subtasks not registered");

            final List<String> sources =
                    FugueStreamTaskRegistry.taskNames().stream()
                            .filter(n -> n.contains(SOURCE_NAME))
                            .sorted()
                            .collect(Collectors.toList());
            assertEquals(SOURCE_PARALLELISM, sources.size());

            // --- Inject the barrier at ONE source subtask only -> partial alignment everywhere. ---
            assertEquals(
                    1,
                    FugueStreamTaskRegistry.injectInto(
                            sources.get(0), MigrationBarrier.createFinalBarrier(MIGRATION_ID)),
                    "should inject into exactly one source subtask");
            awaitCondition(
                    () -> operatorStatesMatch(1, false),
                    Duration.ofSeconds(30),
                    "after one source: each operator subtask should have 1 channel and NOT be aligned");
            assertTrue(
                    operatorStatesMatch(1, false),
                    "no operator subtask should align with only one of two channels barriered");

            // --- Inject the barrier at the SECOND source subtask -> full alignment everywhere. ---
            assertEquals(
                    1,
                    FugueStreamTaskRegistry.injectInto(
                            sources.get(1), MigrationBarrier.createFinalBarrier(MIGRATION_ID)),
                    "should inject into the other source subtask");
            try {
                awaitCondition(
                        () -> operatorStatesMatch(2, true),
                        Duration.ofSeconds(20),
                        "after both sources: each operator subtask should align (barrier on both channels)");
            } catch (AssertionError e) {
                dumpStates("TIMEOUT waiting for full alignment");
                throw e;
            }

            final Set<String> opTasks = operatorTasks();
            assertEquals(OP_PARALLELISM, opTasks.size(), "all operator subtasks should have aligned");
            for (String task : opTasks) {
                final FugueBarrierAligner.AlignmentState s = FugueBarrierAligner.state(task, MIGRATION_ID);
                assertTrue(s.isAligned(), "subtask " + task + " should be aligned");
                assertEquals(2, s.receivedCount(), "subtask " + task + " should have seen the barrier on both channels");
                assertEquals(2, s.expectedChannels(), "subtask " + task + " should expect two input channels");
            }
        } finally {
            job.cancel();
        }
    }

    /** True iff there are exactly OP_PARALLELISM operator subtask states, each matching the args. */
    private static boolean operatorStatesMatch(int received, boolean aligned) {
        final Set<String> opTasks = operatorTasks();
        if (opTasks.size() != OP_PARALLELISM) {
            return false;
        }
        for (String task : opTasks) {
            final FugueBarrierAligner.AlignmentState s = FugueBarrierAligner.state(task, MIGRATION_ID);
            if (s == null || s.receivedCount() != received || s.isAligned() != aligned) {
                return false;
            }
        }
        return true;
    }

    private static void dumpStates(String label) {
        System.out.println("=== FUGUE ALIGNER STATES (" + label + ") ===");
        System.out.println("  registry tasks: " + FugueStreamTaskRegistry.taskNames());
        for (String t : FugueBarrierAligner.tasks(MIGRATION_ID)) {
            final FugueBarrierAligner.AlignmentState s = FugueBarrierAligner.state(t, MIGRATION_ID);
            System.out.println(
                    "    " + t + " -> received=" + s.receivedCount() + " expected=" + s.expectedChannels()
                            + " aligned=" + s.isAligned());
        }
    }

    private static Set<String> operatorTasks() {
        return FugueBarrierAligner.tasks(MIGRATION_ID).stream()
                .filter(n -> n.contains(OP_NAME))
                .collect(Collectors.toSet());
    }

    private static int countTasks(String nameSubstring) {
        return (int)
                FugueStreamTaskRegistry.taskNames().stream()
                        .filter(n -> n.contains(nameSubstring))
                        .count();
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

    /** Emits a few keys per subtask, then stays alive until cancelled. */
    static class GatedSource implements ParallelSourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < 20; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("key-" + i);
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

    static class PassThrough extends KeyedProcessFunction<String, String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            out.collect(value);
        }
    }
}
