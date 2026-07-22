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

package org.apache.flink.fugue.runtime.operator;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-(subtask, key-group) cutover directives for {@code O_new} during the Fugue atomic cutover.
 * After the routing flip, a target subtask receives a migrating key-group's records before the
 * final delta has landed; it must <em>buffer</em> them and only process them once its state is ready.
 * This tracks, per target subtask, the <em>set</em> of key-groups it is buffering and, per key-group,
 * whether replay (the final delta has landed) has been requested.
 *
 * <p>Tracking a set per subtask (not a single key-group) is what lets a single subtask be the target of
 * <em>several concurrent</em> migrations at once — the batch/scale-in scenario where one barrier
 * carries many independent migration plans. {@link FugueKeyedStateOperator} reads this on the
 * mailbox thread for every record, behind a global {@code active} volatile fast-path so non-migrating
 * jobs are unaffected. Written by the migration coordinator at cutover — the single-JVM equivalent of the
 * coordinator→controller RPC (analogous to {@code FugueBackendRegistry} / {@code FugueRoutingOverrides}).
 */
public final class FugueCutover {

    /** target subtask index → set of key-groups it is currently buffering. */
    private static final Map<Integer, Set<Integer>> BUFFERING = new ConcurrentHashMap<>();

    /** target subtask index → set of key-groups whose final delta has landed (ready to replay). */
    private static final Map<Integer, Set<Integer>> REPLAY = new ConcurrentHashMap<>();

    /** Steady-state fast-path flag: false ⇒ the operator skips all cutover bookkeeping. */
    private static volatile boolean active = false;

    private FugueCutover() {}

    /** Direct target {@code subtask} to buffer post-flip records for {@code keyGroup}. */
    public static void armBuffering(int subtask, int keyGroup) {
        BUFFERING.computeIfAbsent(subtask, s -> ConcurrentHashMap.newKeySet()).add(keyGroup);
        active = true;
    }

    /** Whether {@code subtask} is currently buffering {@code keyGroup}. */
    public static boolean isBuffering(int subtask, int keyGroup) {
        final Set<Integer> kgs = BUFFERING.get(subtask);
        return kgs != null && kgs.contains(keyGroup);
    }

    /** Signal that {@code keyGroup}'s final delta has landed, so {@code subtask} may replay it. */
    public static void requestReplay(int subtask, int keyGroup) {
        REPLAY.computeIfAbsent(subtask, s -> ConcurrentHashMap.newKeySet()).add(keyGroup);
    }

    public static boolean replayRequested(int subtask, int keyGroup) {
        final Set<Integer> kgs = REPLAY.get(subtask);
        return kgs != null && kgs.contains(keyGroup);
    }

    /** Called by the operator once it has drained {@code keyGroup}'s buffer and resumed. */
    public static void finishBuffering(int subtask, int keyGroup) {
        final Set<Integer> buffering = BUFFERING.get(subtask);
        if (buffering != null) {
            buffering.remove(keyGroup);
            if (buffering.isEmpty()) {
                BUFFERING.remove(subtask);
            }
        }
        final Set<Integer> replay = REPLAY.get(subtask);
        if (replay != null) {
            replay.remove(keyGroup);
            if (replay.isEmpty()) {
                REPLAY.remove(subtask);
            }
        }
        if (BUFFERING.isEmpty()) {
            active = false;
        }
    }

    /** Drop all cutover state for {@code subtask} (e.g. on operator close or migration abort). */
    public static void finishAll(int subtask) {
        BUFFERING.remove(subtask);
        REPLAY.remove(subtask);
        if (BUFFERING.isEmpty()) {
            active = false;
        }
    }

    public static boolean isActive() {
        return active;
    }

    public static void clear() {
        BUFFERING.clear();
        REPLAY.clear();
        active = false;
    }
}
