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

package org.apache.flink.fugue.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.fugue.runtime.operator.FugueKeyedStateOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * A stateful word-count job whose counting operator is a {@link FugueKeyedStateOperator} backed by
 * RocksDB. Submit it to a Fugue-enabled Flink cluster (see the README) to run a keyed job whose
 * per-key-group state can be migrated between operator instances while the job runs.
 */
public class WordCountWithFugue {

    private static final String STATE_NAME = "word-count";
    private static final int MAX_PARALLELISM = 128;
    private static final int PARALLELISM = 4;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(60_000);
        env.setParallelism(PARALLELISM);
        env.setMaxParallelism(MAX_PARALLELISM);

        final DataStream<String> text = env.addSource(new WordSource()).name("Word Source");

        final DataStream<Tuple2<String, Long>> counts =
                text.flatMap(new Tokenizer())
                        .name("Tokenizer")
                        .keyBy(word -> word)
                        .transform(
                                "Stateful Counter",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}),
                                new FugueKeyedStateOperator<>(new CountFunction()))
                        .setParallelism(PARALLELISM);

        counts.print().name("Print Sink");

        env.execute("Fugue Word Count");
    }

    /** Emits random words at roughly 1000/sec until cancelled. */
    public static class WordSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private static final String[] WORDS = {
            "apache", "flink", "streaming", "fugue", "migration",
            "elasticity", "state", "scalability", "performance",
            "distributed", "system", "data", "processing", "real-time"
        };

        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                final String word = WORDS[random.nextInt(WORDS.length)];
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(word);
                }
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Splits the stream into words. */
    public static class Tokenizer implements FlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<String> out) {
            for (String token : value.toLowerCase().split("\\W+")) {
                if (!token.isEmpty()) {
                    out.collect(token);
                }
            }
        }
    }

    /** Maintains a running count per word in keyed RocksDB state. */
    public static class CountFunction extends KeyedProcessFunction<String, String, Tuple2<String, Long>> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>(STATE_NAME, Long.class));
        }

        @Override
        public void processElement(String word, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            final Long current = count.value();
            final long next = (current == null ? 0L : current) + 1L;
            count.update(next);
            out.collect(Tuple2.of(word, next));
        }
    }
}
