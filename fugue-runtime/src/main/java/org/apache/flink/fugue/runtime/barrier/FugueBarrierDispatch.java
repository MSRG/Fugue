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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static sink that the patched input path ({@code AbstractStreamTaskNetworkInput#processEvent}) calls
 * when it recognizes a {@link MigrationBarrier} on an input channel. It records, per
 * {@code (taskName, migrationId)}, the set of input channels that have delivered the barrier — the
 * per-channel alignment progress at each downstream subtask.
 *
 * <p>{@code processEvent} has no task identity in scope, so the patched {@code StreamTask} binds its
 * {@code getName()} (the task-name-with-subtask) to its mailbox thread via {@link #bindTaskName} at
 * startup; {@code processEvent} runs on that same thread, so {@link #onBarrier} recovers the subtask
 * identity from the thread-local. This dispatch observes injection + in-band propagation; alignment
 * across a subtask's input channels is tracked by {@link FugueBarrierAligner}, the routing flip by
 * {@link FugueRoutingOverrides}, and the cutover (post-barrier buffering/replay) by the operator
 * ({@code FugueKeyedStateOperator}/{@code FugueCutover}).
 */
public final class FugueBarrierDispatch {

    /** key = taskName + '#' + migrationId → set of channels that delivered the barrier. */
    private static final Map<String, Set<InputChannelInfo>> ARRIVALS = new ConcurrentHashMap<>();

    /** The current mailbox thread's task name, bound by the patched {@code StreamTask} at startup. */
    private static final ThreadLocal<String> CURRENT_TASK = new ThreadLocal<>();

    private FugueBarrierDispatch() {}

    /** Bind the calling (mailbox) thread to its task name; called by the patched {@code StreamTask}. */
    public static void bindTaskName(String taskName) {
        CURRENT_TASK.set(taskName);
    }

    /** Called on the mailbox thread when a migration barrier arrives on {@code channel}. */
    public static void onBarrier(MigrationBarrier barrier, InputChannelInfo channel) {
        ARRIVALS
                .computeIfAbsent(
                        key(currentTaskName(), barrier.getMigrationId()), k -> ConcurrentHashMap.newKeySet())
                .add(channel);
    }

    /** Number of distinct input channels that have delivered the barrier to this task so far. */
    public static int channelsSeen(String taskName, long migrationId) {
        final Set<InputChannelInfo> channels = ARRIVALS.get(key(taskName, migrationId));
        return channels == null ? 0 : channels.size();
    }

    /** Task names (with subtask) that have seen the barrier for {@code migrationId}. */
    public static Set<String> tasksThatSawBarrier(long migrationId) {
        final String suffix = "#" + migrationId;
        final Set<String> names = ConcurrentHashMap.newKeySet();
        for (String key : ARRIVALS.keySet()) {
            if (key.endsWith(suffix)) {
                names.add(key.substring(0, key.length() - suffix.length()));
            }
        }
        return names;
    }

    public static void clear() {
        ARRIVALS.clear();
    }

    /** The task name bound to the current mailbox thread (shared with {@link FugueBarrierAligner}). */
    public static String currentTaskName() {
        final String bound = CURRENT_TASK.get();
        return bound != null ? bound : Thread.currentThread().getName();
    }

    private static String key(String taskName, long migrationId) {
        return taskName + "#" + migrationId;
    }
}
