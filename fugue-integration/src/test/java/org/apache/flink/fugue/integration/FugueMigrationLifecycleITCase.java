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

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.state.MigrationContext;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;
import org.apache.flink.fugue.coordinator.rpc.FugueCoordinatorEndpoint;
import org.apache.flink.fugue.integration.taskmanager.FugueTaskExecutorService;
import org.apache.flink.fugue.runtime.controller.MigrationController;
import org.apache.flink.fugue.runtime.transfer.StateMigrationService;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end control-plane test: a real {@link FugueCoordinatorEndpoint} drives a migration through
 * its full lifecycle (INIT → PRE_COPY → AWAIT_BARRIER → FINALIZING → COMMITTED) over real
 * Flink RPC against two real {@link FugueTaskExecutorService} controller endpoints, using a
 * deterministic in-memory {@link StateMigrationService} (the actual state-transfer mechanism is
 * exercised separately; here we verify the protocol + RPC wiring). The barrier alignment is invoked
 * programmatically because there is no data path in this control-plane test.
 */
class FugueMigrationLifecycleITCase {

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
    void coordinatorDrivesFullMigrationLifecycleOverRpc() throws Exception {
        final JobID jobId = new JobID();

        final FugueCoordinatorEndpoint coordinator =
                new FugueCoordinatorEndpoint(rpcService, jobId, new PolicyConfiguration());
        coordinator.start();

        final FugueTaskExecutorService sourceTm =
                new FugueTaskExecutorService(
                        rpcService,
                        "tm-source",
                        new MigrationController("tm-source", new InMemoryStateMigration()));
        final FugueTaskExecutorService targetTm =
                new FugueTaskExecutorService(
                        rpcService,
                        "tm-target",
                        new MigrationController("tm-target", new InMemoryStateMigration()));
        sourceTm.start();
        targetTm.start();
        sourceTm.setCoordinatorAddress(coordinator.getAddress());
        targetTm.setCoordinatorAddress(coordinator.getAddress());

        try {
            final MigrationPlan plan =
                    new MigrationPlan(
                            5,
                            new OperatorID(),
                            new MigrationPlan.OperatorInstance(0, sourceTm.getAddress()),
                            new MigrationPlan.OperatorInstance(1, targetTm.getAddress()),
                            jobId,
                            4096L);

            final List<Long> ids =
                    coordinator.startMigrations(Collections.singletonList(plan)).get(10, TimeUnit.SECONDS);
            assertEquals(1, ids.size());
            final long migrationId = ids.get(0);

            // Capture the completion future before the migration finishes (it is created at start).
            final java.util.concurrent.CompletableFuture<MigrationState> completion =
                    coordinator.getMigrationManager().getCompletion(migrationId);

            // The source's pre-copy converges and reports back over real RPC -> AWAIT_BARRIER.
            awaitState(coordinator, migrationId, MigrationState.AWAIT_BARRIER, Duration.ofSeconds(15));

            // No data path in this control-plane test: trigger the cutover programmatically.
            coordinator.notifyBarrierAligned(migrationId);

            // Finalization (final delta + commit over RPC) drives the migration to COMMITTED.
            final MigrationState terminal = completion.get(15, TimeUnit.SECONDS);
            assertEquals(MigrationState.COMMITTED, terminal);
        } finally {
            sourceTm.closeAsync().get(10, TimeUnit.SECONDS);
            targetTm.closeAsync().get(10, TimeUnit.SECONDS);
            coordinator.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    private static void awaitState(
            FugueCoordinatorEndpoint coordinator,
            long migrationId,
            MigrationState expected,
            Duration timeout)
            throws InterruptedException {
        final long deadline = System.nanoTime() + timeout.toNanos();
        MigrationState last = null;
        while (System.nanoTime() < deadline) {
            final MigrationContext ctx = coordinator.getMigrationManager().getMigration(migrationId);
            if (ctx != null) {
                last = ctx.getCurrentState();
                if (last == expected) {
                    return;
                }
            }
            Thread.sleep(25);
        }
        fail("migration " + migrationId + " did not reach " + expected + " (last state: " + last + ")");
    }

    /** Deterministic in-memory transfer: converges on the first pre-copy round, no sockets/RocksDB. */
    private static final class InMemoryStateMigration implements StateMigrationService {
        @Override
        public void prepareSource(long migrationId, int partitionId, String targetAddress, int targetPort) {}

        @Override
        public void prepareTarget(long migrationId, int partitionId, int listenPort) {}

        @Override
        public StateMigrationService.TransferResult transferSnapshot(
                long migrationId, int partitionId, int roundNumber, long rateLimitBytesPerSec) {
            // deltaSize 0 is below the controller's convergence threshold -> converges immediately.
            return new StateMigrationService.TransferResult(1024L, 0L);
        }

        @Override
        public StateMigrationService.TransferResult transferFinalDelta(long migrationId, int partitionId) {
            return new StateMigrationService.TransferResult(256L, 0L);
        }

        @Override
        public void stopDeltaLogging(long migrationId) {}

        @Override
        public void deletePartitionState(long migrationId, int partitionId) {}

        @Override
        public void abortSource(long migrationId) {}

        @Override
        public void abortTarget(long migrationId) {}

        @Override
        public void shutdown() {}
    }
}
