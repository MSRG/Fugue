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

package org.apache.flink.fugue.integration.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages;
import org.apache.flink.fugue.coordinator.rpc.FugueRpcGateway;
import org.apache.flink.fugue.runtime.barrier.FugueStreamTaskRegistry;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rpc.TestingRpcService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-node mechanism (MN4): the coordinator's cross-node barrier injection. Over real Flink RPC, an
 * {@code InjectBarrier} command sent to a {@link FugueTaskExecutorService} causes it to build the
 * migration barrier and inject it into its <em>local</em> source operators via
 * {@link FugueStreamTaskRegistry} (the per-node injection that flips that node's routing override
 * in-band). This verifies the receiving end of the cross-node injection; the coordinator fanning the
 * command out to each source-hosting controller is the cluster-specific orchestration covered by the
 * multi-node deployment guide.
 */
class FugueCrossNodeInjectionITCase {

    private TestingRpcService rpcService;

    @BeforeEach
    void setUp() {
        rpcService = new TestingRpcService();
        FugueStreamTaskRegistry.clear();
    }

    @AfterEach
    void tearDown() throws Exception {
        FugueStreamTaskRegistry.clear();
        if (rpcService != null) {
            rpcService.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void controllerInjectsBarrierIntoLocalSourcesOnCommand() throws Exception {
        final AtomicReference<MigrationBarrier> injected = new AtomicReference<>();
        // Stand in for a running source StreamTask on this node (the patched StreamTask registers itself).
        FugueStreamTaskRegistry.register("Source: fugue-source (1/1)#0", injected::set);

        final FugueTaskExecutorService controller = new FugueTaskExecutorService(rpcService, "tm-1");
        controller.start();
        try {
            final MigrationPlan plan =
                    new MigrationPlan(
                            7,
                            new OperatorID(),
                            new MigrationPlan.OperatorInstance(0, "tm-a"),
                            new MigrationPlan.OperatorInstance(1, "tm-b"),
                            new JobID(),
                            0L);
            final MigrationMessages.InjectBarrier command =
                    new MigrationMessages.InjectBarrier(
                            99L, Collections.singletonList(plan), "fugue-source", false);

            final FugueRpcGateway.TaskManagerMigrationGateway gateway =
                    rpcService
                            .connect(controller.getAddress(), FugueRpcGateway.TaskManagerMigrationGateway.class)
                            .get(10, TimeUnit.SECONDS);
            final MigrationMessages.MigrationMessage response =
                    gateway.handleMigrationMessage(command).get(10, TimeUnit.SECONDS);

            assertTrue(
                    ((MigrationMessages.MigrationResponse) response).isSuccess(),
                    "controller should report a successful injection");
            assertNotNull(injected.get(), "barrier should have been injected into the local source");
            assertEquals(99L, injected.get().getMigrationId());
            assertEquals(1, injected.get().getMigrationPlans().size());
            assertEquals(7, injected.get().getMigrationPlans().get(0).getPartitionId());
        } finally {
            controller.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }
}
