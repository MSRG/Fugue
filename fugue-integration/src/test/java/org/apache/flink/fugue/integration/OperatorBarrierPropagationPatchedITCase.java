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
import org.apache.flink.fugue.runtime.barrier.FugueBarrierDispatch;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Patched fork: proves the in-band migration barrier actually flows through the
 * <em>real, patched</em> Flink network stack. On a MiniCluster (backed by the {@code build-flink.sh}
 * patched runtime), a {@link MigrationBarrier} injected at the source subtasks is broadcast downstream,
 * recognized by the patched {@code AbstractStreamTaskNetworkInput#processEvent}, and per-channel
 * aligned at every downstream operator subtask.
 *
 * <p>Tagged {@code "patched"} so it is excluded from the clean {@code mvn verify} (on un-patched Flink
 * the event is silently dropped); run with {@code mvn -o verify -P patched} after a fork build.
 *
 * <p>The injection trigger ({@link FugueStreamTaskRegistry}, driven from the test) plays the role of the
 * coordinator→source RPC; the parts exercised here are the patched input-path recognition,
 * in-band source→downstream propagation, and per-channel alignment-detection.
 */
@Tag("patched")
class OperatorBarrierPropagationPatchedITCase {

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
    void migrationBarrierPropagatesAndAlignsAcrossAllInputChannels() throws Exception {
        FugueBarrierDispatch.clear();
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

        final JobClient job = env.executeAsync("fugue-phase3-barrier-propagation");
        try {
            assertTrue(SOURCE_EMITTED.await(30, TimeUnit.SECONDS), "sources did not start");
            // All source + operator subtasks are deployed and running (registered on their mailbox thread).
            awaitCondition(
                    () ->
                            countTasks(SOURCE_NAME) == SOURCE_PARALLELISM
                                    && countTasks(OP_NAME) == OP_PARALLELISM,
                    Duration.ofSeconds(30),
                    "not all source/operator subtasks registered");

            // Inject the in-band migration barrier at the source subtasks (in place of the coordinator RPC).
            final int injected =
                    FugueStreamTaskRegistry.injectInto(SOURCE_NAME, MigrationBarrier.createFinalBarrier(MIGRATION_ID));
            assertEquals(SOURCE_PARALLELISM, injected, "barrier should be injected into both source subtasks");

            // Each downstream operator subtask must receive + recognize the barrier on ALL its input
            // channels (one per source subtask) — real in-band propagation + per-channel alignment.
            awaitCondition(
                    OperatorBarrierPropagationPatchedITCase::allOperatorSubtasksAligned,
                    Duration.ofSeconds(30),
                    "barrier did not align across all input channels at every operator subtask");

            final Set<String> opTasks =
                    FugueBarrierDispatch.tasksThatSawBarrier(MIGRATION_ID).stream()
                            .filter(n -> n.contains(OP_NAME))
                            .collect(Collectors.toSet());
            assertEquals(OP_PARALLELISM, opTasks.size(), "all operator subtasks should have seen the barrier");
            for (String task : opTasks) {
                assertEquals(
                        SOURCE_PARALLELISM,
                        FugueBarrierDispatch.channelsSeen(task, MIGRATION_ID),
                        "operator subtask " + task + " should align the barrier across both input channels");
            }
        } finally {
            job.cancel();
        }
    }

    private static boolean allOperatorSubtasksAligned() {
        final Set<String> opTasks =
                FugueBarrierDispatch.tasksThatSawBarrier(MIGRATION_ID).stream()
                        .filter(n -> n.contains(OP_NAME))
                        .collect(Collectors.toSet());
        if (opTasks.size() < OP_PARALLELISM) {
            return false;
        }
        for (String task : opTasks) {
            if (FugueBarrierDispatch.channelsSeen(task, MIGRATION_ID) < SOURCE_PARALLELISM) {
                return false;
            }
        }
        return true;
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

    /** Emits a fixed set of keys (per subtask), then stays alive until cancelled. */
    static class GatedSource implements ParallelSourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < 50; i++) {
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
