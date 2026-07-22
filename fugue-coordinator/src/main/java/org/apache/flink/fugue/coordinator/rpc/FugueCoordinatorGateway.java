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

package org.apache.flink.fugue.coordinator.rpc;

import org.apache.flink.fugue.common.rpc.MigrationMessages;
import org.apache.flink.runtime.rpc.RpcGateway;

/**
 * RPC gateway exposed by the Fugue coordinator (hosted in the JobMaster) for callbacks from the
 * per-TaskManager migration controllers. This is the controller -&gt; coordinator direction; the
 * coordinator -&gt; controller direction uses {@link FugueRpcGateway.TaskManagerMigrationGateway}.
 */
public interface FugueCoordinatorGateway extends RpcGateway {

    /**
     * Report a migration message from a TaskManager-side controller (e.g. {@code
     * PreCopyRoundComplete}, {@code FinalDeltaComplete}, {@code StateUpdate}, {@code
     * MigrationStats}). Fire-and-forget.
     */
    void handleMigrationMessage(MigrationMessages.MigrationMessage message);

    /**
     * Notify the coordinator that the migration barrier has aligned across all participating
     * operator instances for the given migration (drives the cutover -&gt; finalize transition).
     */
    void notifyBarrierAligned(long migrationId);
}
