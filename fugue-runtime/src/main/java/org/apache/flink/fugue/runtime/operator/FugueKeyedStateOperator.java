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
import org.apache.flink.fugue.runtime.barrier.FugueRoutingOverrides;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link KeyedProcessOperator} that publishes its live {@link RocksDBKeyedStateBackend} to
 * {@link FugueBackendRegistry} on {@code open()} — the operator-level seam through which the Fugue
 * Migration Controller reaches the local state backend — and that performs the target-side
 * <b>post-barrier buffering</b> of the Fugue atomic cutover.
 *
 * <p>During a cutover, once the upstream routing has flipped (see {@code FugueRoutingOverrides}), this
 * operator's subtask {@code O_new} starts receiving the migrating key-group's records before the final
 * delta has been applied. While {@link FugueCutover} marks this subtask as buffering that key-group, such
 * records are <em>held</em> in arrival order; every other key (including this subtask's own key-groups) is
 * processed normally, so non-migrating partitions are never blocked. When the coordinator signals replay
 * (the final delta has landed), the held records are processed in order on top of the now-consistent
 * state, then normal processing resumes. The buffering machinery is entirely behind
 * {@link FugueCutover#isActive()}, so jobs with no migration in progress are unaffected.
 *
 * <p>By the time {@code super.open()} returns, the user function's {@code open()} has registered its
 * keyed state, so the backend (and its column families) exist and can be published.
 */
public class FugueKeyedStateOperator<K, IN, OUT> extends KeyedProcessOperator<K, IN, OUT> {

    private static final long serialVersionUID = 1L;

    private transient int subtaskIndex;
    private transient AbstractKeyedStateBackend<?> keyedBackend;
    /** Per-key-group hold buffers (mailbox-thread only); a subtask can buffer several migrating kgs. */
    private transient Map<Integer, Deque<StreamRecord<IN>>> heldByKg;

    public FugueKeyedStateOperator(KeyedProcessFunction<K, IN, OUT> function) {
        super(function);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        if (context.isRestored()) {
            // Global restart (recovery from a checkpoint): any in-flight migration is abandoned and the
            // system reverts to the pre-migration topology. Reset Fugue's transient,
            // non-checkpointed migration state so the recovered job does not act on a stale routing flip
            // or buffering directive. Hosted key-groups reset naturally with the freshly-built backends.
            FugueRoutingOverrides.clear();
            FugueCutover.clear();
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        heldByKg = new HashMap<>();
        final KeyedStateBackend<?> backend = getKeyedStateBackend();
        if (backend instanceof RocksDBKeyedStateBackend) {
            FugueBackendRegistry.register(subtaskIndex, (RocksDBKeyedStateBackend<?>) backend);
        }
        if (backend instanceof AbstractKeyedStateBackend) {
            keyedBackend = (AbstractKeyedStateBackend<?>) backend;
        }
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (FugueCutover.isActive() && keyedBackend != null) {
            // The element's key context (and thus its key-group) is set by the framework's
            // setKeyContextElement1(element) call immediately before processElement.
            final int kg = keyedBackend.getCurrentKeyGroupIndex();
            if (FugueCutover.isBuffering(subtaskIndex, kg)) {
                if (FugueCutover.replayRequested(subtaskIndex, kg)) {
                    // Final delta has landed: replay this key-group's held records in arrival order,
                    // then process this record live on top of the now-consistent state (O_new applies
                    // the final delta, then processes buffered + new records).
                    replayHeld(kg);
                    FugueCutover.finishBuffering(subtaskIndex, kg);
                    setKeyContextElement1(element);
                    super.processElement(element);
                } else {
                    heldByKg.computeIfAbsent(kg, k -> new ArrayDeque<>()).addLast(element);
                }
                return;
            }
        }
        super.processElement(element);
    }

    private void replayHeld(int keyGroup) throws Exception {
        final Deque<StreamRecord<IN>> held = heldByKg.remove(keyGroup);
        if (held == null) {
            return;
        }
        StreamRecord<IN> r;
        while ((r = held.pollFirst()) != null) {
            setKeyContextElement1(r);
            super.processElement(r);
        }
    }

    @Override
    public void close() throws Exception {
        FugueBackendRegistry.unregister(subtaskIndex);
        FugueCutover.finishAll(subtaskIndex);
        super.close();
    }
}
