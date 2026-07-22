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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-JVM registry of running tasks that can inject a migration barrier into their output, keyed by
 * task name. The patched {@code StreamTask} registers itself on startup. This is the single-JVM
 * equivalent of the coordinator→source {@code OperatorEvent} RPC that injects the barrier in a real
 * cluster (analogous to {@code FugueBackendRegistry}). The patched input-path recognition, in-band
 * propagation, and alignment are what do the work, not this trigger.
 */
public final class FugueStreamTaskRegistry {

    private static final Map<String, FugueBarrierInjector> INJECTORS = new ConcurrentHashMap<>();

    private FugueStreamTaskRegistry() {}

    public static void register(String taskName, FugueBarrierInjector injector) {
        INJECTORS.put(taskName, injector);
    }

    public static void unregister(String taskName) {
        INJECTORS.remove(taskName);
    }

    /**
     * Inject the barrier into every registered task whose name contains {@code taskNameSubstring}
     * (e.g. "Source"); returns how many tasks it was injected into.
     */
    public static int injectInto(String taskNameSubstring, MigrationBarrier barrier) {
        int injected = 0;
        for (Map.Entry<String, FugueBarrierInjector> entry : INJECTORS.entrySet()) {
            if (entry.getKey().contains(taskNameSubstring)) {
                entry.getValue().inject(barrier);
                injected++;
            }
        }
        return injected;
    }

    public static Set<String> taskNames() {
        return INJECTORS.keySet();
    }

    public static void clear() {
        INJECTORS.clear();
    }
}
