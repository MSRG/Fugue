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

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-JVM registry of live keyed RocksDB backends, keyed by subtask index. On a single-JVM MiniCluster
 * this stands in for "the per-TaskExecutor Migration Controller knows its local operator backends"
 * (the eventual cluster wiring patches the operator/{@code StreamTask} to register with the local
 * controller). With one JVM and one TaskExecutor, JVM-global is equivalent to per-TaskExecutor.
 *
 * <p>This is the only test/prototype-flavoured discovery scaffolding in the increment — the operator,
 * accessor, transfer primitive, and migration service it connects are genuine.
 */
public final class FugueBackendRegistry {

    private static final Map<Integer, RocksDBKeyedStateBackend<?>> BACKENDS = new ConcurrentHashMap<>();

    private FugueBackendRegistry() {}

    public static void register(int subtaskIndex, RocksDBKeyedStateBackend<?> backend) {
        BACKENDS.put(subtaskIndex, backend);
    }

    public static void unregister(int subtaskIndex) {
        BACKENDS.remove(subtaskIndex);
    }

    public static RocksDBKeyedStateBackend<?> get(int subtaskIndex) {
        return BACKENDS.get(subtaskIndex);
    }

    public static Set<Integer> subtasks() {
        return BACKENDS.keySet();
    }

    public static void clear() {
        BACKENDS.clear();
    }
}
