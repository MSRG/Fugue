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

package org.apache.flink.fugue.runtime.barrier;

import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-subtask migration-barrier <em>alignment</em> at the input gate — the consistent-cut decision of
 * the Fugue cutover, mirroring the tracking half of Flink's checkpoint-barrier handlers.
 * Called from the patched {@code CheckpointedInputGate#handleEvent} on the mailbox thread; it records,
 * per subtask, the input channels that have delivered the barrier, and marks the subtask
 * <em>aligned</em> once the barrier has arrived on <em>all</em> of its input channels (the point at
 * which the cutover may proceed: every pre-barrier record has been seen on every channel).
 *
 * <p><b>Scope note:</b> this detects the aligned cut-point but does <em>not</em> physically
 * hold post-barrier records. Flink's channel blocking ({@code CheckpointableInput.blockConsumption} /
 * {@code resumeConsumption}) is coupled to the checkpoint-barrier subpartition state — for local
 * channels {@code PipelinedSubpartition.resumeConsumption()} asserts {@code isBlocked}, which is set
 * only when a real {@code CheckpointBarrier} passes through. Truly holding records for a custom barrier
 * therefore needs deeper machinery (a Fugue-side per-channel record buffer in the input path, or making
 * the barrier ride the checkpoint mechanism) and is deferred to the cross-range cutover finale. This
 * class is at the flink-runtime level so it can be vendored into flink-runtime; the per-subtask
 * identity comes from {@link FugueBarrierDispatch#currentTaskName()}.
 */
public final class FugueBarrierAligner {

    /** Observable per-subtask alignment progress for a migration. */
    public static final class AlignmentState {
        private final int expectedChannels;
        private final Set<InputChannelInfo> received = ConcurrentHashMap.newKeySet();
        private volatile boolean aligned;

        AlignmentState(int expectedChannels) {
            this.expectedChannels = expectedChannels;
        }

        public boolean isAligned() {
            return aligned;
        }

        public int receivedCount() {
            return received.size();
        }

        public int expectedChannels() {
            return expectedChannels;
        }
    }

    /** key = taskName + '#' + migrationId. */
    private static final Map<String, AlignmentState> STATES = new ConcurrentHashMap<>();

    private FugueBarrierAligner() {}

    /**
     * Record a migration barrier on {@code channel} at the subtask owning {@code gate}, and mark the
     * subtask aligned once the barrier has arrived on every one of its input channels.
     */
    public static void onBarrier(
            MigrationBarrier barrier, InputChannelInfo channel, CheckpointableInput gate) {
        final String task = FugueBarrierDispatch.currentTaskName();
        final AlignmentState state =
                STATES.computeIfAbsent(
                        key(task, barrier.getMigrationId()),
                        k -> new AlignmentState(gate.getChannelInfos().size()));
        state.received.add(channel);
        if (state.received.size() >= state.expectedChannels) {
            state.aligned = true;
        }
    }

    public static AlignmentState state(String taskName, long migrationId) {
        return STATES.get(key(taskName, migrationId));
    }

    /** Task names (with subtask) that have an alignment state for {@code migrationId}. */
    public static Set<String> tasks(long migrationId) {
        final String suffix = "#" + migrationId;
        final Set<String> names = ConcurrentHashMap.newKeySet();
        for (String stateKey : STATES.keySet()) {
            if (stateKey.endsWith(suffix)) {
                names.add(stateKey.substring(0, stateKey.length() - suffix.length()));
            }
        }
        return names;
    }

    public static void clear() {
        STATES.clear();
    }

    private static String key(String taskName, long migrationId) {
        return taskName + "#" + migrationId;
    }
}
