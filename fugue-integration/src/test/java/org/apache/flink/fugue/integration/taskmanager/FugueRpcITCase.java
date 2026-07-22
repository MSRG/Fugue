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

import org.apache.flink.fugue.common.rpc.MigrationMessages.AbortMigration;
import org.apache.flink.fugue.common.rpc.MigrationMessages.MigrationMessage;
import org.apache.flink.fugue.common.rpc.MigrationMessages.MigrationResponse;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PreCopyRoundComplete;
import org.apache.flink.fugue.coordinator.rpc.FugueCoordinatorGateway;
import org.apache.flink.fugue.coordinator.rpc.FugueRpcGateway;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the real bidirectional coordinator&lt;-&gt;controller RPC over a real Flink {@link
 * TestingRpcService} (backed by a local Pekko RPC service): the controller -&gt; coordinator
 * callback (previously a {@code TODO} no-op) and the coordinator -&gt; controller command path.
 */
class FugueRpcITCase {

    private TestingRpcService rpcService;

    @BeforeEach
    void setUp() {
        rpcService = new TestingRpcService();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (rpcService != null) {
            rpcService.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void controllerReportsReachTheCoordinator() throws Exception {
        RecordingCoordinatorGateway coordinator = new RecordingCoordinatorGateway("fugue-coordinator");
        rpcService.registerGateway(coordinator.getAddress(), coordinator);

        FugueTaskExecutorService controller = new FugueTaskExecutorService(rpcService, "tm-1");
        controller.start();
        try {
            controller.setCoordinatorAddress(coordinator.getAddress());

            // This is the path that used to be a no-op TODO.
            controller.sendToCoordinator(new PreCopyRoundComplete(7L, 2, 1024L, 4096L, true));

            MigrationMessage received = coordinator.received.poll(10, TimeUnit.SECONDS);
            assertNotNull(received, "coordinator never received the controller's report");
            assertTrue(received instanceof PreCopyRoundComplete);
            assertEquals(7L, received.getMigrationId());
            assertTrue(((PreCopyRoundComplete) received).isConverged());
        } finally {
            controller.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void coordinatorCommandsReachTheControllerAndAreAcked() throws Exception {
        FugueTaskExecutorService controller = new FugueTaskExecutorService(rpcService, "tm-1");
        controller.start();
        try {
            FugueRpcGateway coordinatorToTm = new FugueRpcGateway(rpcService);

            // Real RPC round-trip (serialized) to the started controller endpoint.
            MigrationMessage response =
                    coordinatorToTm
                            .sendToTaskManager(
                                    controller.getAddress(), new AbortMigration(42L, "test"))
                            .get(10, TimeUnit.SECONDS);

            assertTrue(response instanceof MigrationResponse);
            assertTrue(((MigrationResponse) response).isSuccess());
            assertEquals(42L, response.getMigrationId());
        } finally {
            controller.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    /** Records the messages the controller pushes to the coordinator. */
    private static final class RecordingCoordinatorGateway implements FugueCoordinatorGateway {
        final BlockingQueue<MigrationMessage> received = new LinkedBlockingQueue<>();
        private final String address;

        RecordingCoordinatorGateway(String address) {
            this.address = address;
        }

        @Override
        public void handleMigrationMessage(MigrationMessage message) {
            received.add(message);
        }

        @Override
        public void notifyBarrierAligned(long migrationId) {}

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public String getHostname() {
            return "localhost";
        }
    }
}
