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
import org.apache.flink.fugue.common.core.MigrationPlan;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global (per-JVM) routing-override table for the Fugue cutover's <em>atomic routing flip</em>:
 * it maps a migrating key-group to the downstream channel (the target subtask) that should now
 * receive its records. The patched {@code KeyGroupStreamPartitioner#selectChannel} consults
 * {@link #channelFor} for every record; an {@code active} fast-path makes that a single volatile read
 * (no map lookup, no behaviour change) whenever no migration is in progress, so non-migrating jobs are
 * unaffected.
 *
 * <p>This lives at the flink-runtime level (it references only flink-runtime-level Fugue types —
 * {@link MigrationBarrier}/{@link MigrationPlan}, themselves in fugue-common) so it vendors into
 * flink-runtime and the flink-streaming-java partitioner can see it. It is the single-JVM equivalent of
 * the per-TaskManager override state that the migration barrier installs at each upstream operator in a
 * real cluster — analogous to {@code FugueBackendRegistry} / {@code FugueStreamTaskRegistry}.
 *
 * <p>The flip is installed in-band on the upstream's mailbox thread via {@link #applyFromBarrier} when
 * the {@link MigrationBarrier} is broadcast (see the patched {@code StreamTask} injector): records
 * emitted before the barrier keep the old routing, records after take the override.
 */
public final class FugueRoutingOverrides {

    /** keyGroup → target channel (downstream subtask) for in-progress migrations. */
    private static final Map<Integer, Integer> OVERRIDES = new ConcurrentHashMap<>();

    /** Steady-state fast-path flag: false ⇒ {@link #channelFor} is a single volatile read. */
    private static volatile boolean active = false;

    private FugueRoutingOverrides() {}

    /** The channel for {@code keyGroup}: its override if one is set, otherwise {@code defaultChannel}. */
    public static int channelFor(int keyGroup, int defaultChannel) {
        if (!active) {
            return defaultChannel;
        }
        final Integer override = OVERRIDES.get(keyGroup);
        return override != null ? override : defaultChannel;
    }

    /** Redirect a key-group's records to {@code channel} (the cutover flip). */
    public static void setOverride(int keyGroup, int channel) {
        OVERRIDES.put(keyGroup, channel);
        active = true;
    }

    /**
     * Install the routing flip carried by a migration barrier: each plan's key-group is redirected to
     * that plan's target subtask. A final barrier (no plans) is a no-op.
     */
    public static void applyFromBarrier(MigrationBarrier barrier) {
        for (MigrationPlan plan : barrier.getMigrationPlans()) {
            setOverride(plan.getPartitionId(), plan.getTargetInstance().getSubtaskIndex());
        }
    }

    public static Integer getOverride(int keyGroup) {
        return OVERRIDES.get(keyGroup);
    }

    public static void clearOverride(int keyGroup) {
        OVERRIDES.remove(keyGroup);
        if (OVERRIDES.isEmpty()) {
            active = false;
        }
    }

    public static boolean isActive() {
        return active;
    }

    public static void clear() {
        OVERRIDES.clear();
        active = false;
    }
}
