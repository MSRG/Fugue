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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Substrate test: runs a real keyed, RocksDB-backed streaming job on a local Flink
 * MiniCluster and verifies that keyed state is maintained correctly across the cluster's task
 * slots. Fugue migrations run on top of a job shaped like this, so this test pins
 * down a known-good baseline (real Flink runtime, real RocksDB keyed state) on this machine.
 */
class KeyedStateMiniClusterITCase {

    private static final int SLOTS = 4;
    private static final int MAX_PARALLELISM = 128;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(SLOTS)
                            .build());

    /** Collected in-JVM (MiniCluster runs in this JVM), so a static sink is safe. */
    static final Queue<Tuple2<String, Long>> SINK = new ConcurrentLinkedQueue<>();

    @Test
    void keyedRocksDbStateIsMaintainedAcrossTheCluster() throws Exception {
        SINK.clear();

        // Deterministic, bounded input: each key repeated a known number of times.
        final Map<String, Integer> expectedCounts = new HashMap<>();
        final List<String> input = new ArrayList<>();
        final String[] keys = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (int i = 0; i < keys.length; i++) {
            int n = 10 + i; // a -> 10, b -> 11, ...
            expectedCounts.put(keys[i], n);
            for (int j = 0; j < n; j++) {
                input.add(keys[i]);
            }
        }

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(SLOTS);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        final DataStreamSource<String> source = env.fromCollection(input);
        source.keyBy(k -> k).map(new RunningCount()).addSink(new CollectSink());

        env.execute("fugue-phase0-keyed-rocksdb");

        // For each key, the maximum running count emitted equals the number of input records for it.
        final Map<String, Long> maxPerKey = new HashMap<>();
        for (Tuple2<String, Long> t : SINK) {
            maxPerKey.merge(t.f0, t.f1, Math::max);
        }
        assertFalse(maxPerKey.isEmpty(), "sink received no records");
        for (Map.Entry<String, Integer> e : expectedCounts.entrySet()) {
            assertEquals(
                    (long) e.getValue(),
                    (long) maxPerKey.getOrDefault(e.getKey(), -1L),
                    "wrong final keyed-state count for key " + e.getKey());
        }
    }

    /** Stateful running count per key, backed by keyed (RocksDB) state. */
    static class RunningCount extends RichMapFunction<String, Tuple2<String, Long>> {
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
        }

        @Override
        public Tuple2<String, Long> map(String key) throws Exception {
            final Long current = count.value();
            final long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            return Tuple2.of(key, next);
        }
    }

    static class CollectSink implements SinkFunction<Tuple2<String, Long>> {
        @Override
        public void invoke(Tuple2<String, Long> value, Context context) {
            SINK.add(Tuple2.of(value.f0, value.f1));
        }
    }
}
