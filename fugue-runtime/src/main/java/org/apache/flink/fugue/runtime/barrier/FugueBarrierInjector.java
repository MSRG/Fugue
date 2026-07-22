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

/**
 * Injects a {@link MigrationBarrier} into a running task's output (the upstream broadcast of the Fugue
 * cutover). Defined at the flink-runtime level so vendored Fugue code can hold it without depending on
 * flink-streaming-java: the patched {@code StreamTask} registers a lambda that broadcasts the barrier
 * on its mailbox thread, and {@link FugueStreamTaskRegistry} stores only this interface.
 */
@FunctionalInterface
public interface FugueBarrierInjector {

    /** Broadcast the barrier downstream. Implementations must run on the task's mailbox thread. */
    void inject(MigrationBarrier barrier);
}
