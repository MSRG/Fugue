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

package org.apache.flink.fugue.coordinator.manager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages.AbortMigration;
import org.apache.flink.fugue.common.rpc.MigrationMessages.CommitMigration;
import org.apache.flink.fugue.common.rpc.MigrationMessages.MigrationMessage;
import org.apache.flink.fugue.common.rpc.MigrationMessages.MigrationResponse;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PrepareMigrationSource;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PrepareMigrationTarget;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PreCopyRoundComplete;
import org.apache.flink.fugue.common.rpc.MigrationMessages.StartPreCopy;
import org.apache.flink.fugue.common.rpc.MigrationMessages.TransferFinalDelta;
import org.apache.flink.fugue.common.state.MigrationContext;
import org.apache.flink.fugue.common.state.MigrationState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the event-driven {@link MigrationManager} state machine. We verify the manager advances
 * purely in response to protocol events delivered through the RPC gateway, and that the divergence
 * safeguard aborts a non-converging migration.
 *
 * <p>The gateway is an in-test implementation that records outbound messages and returns
 * success responses synchronously, so the manager's orchestration logic is exercised end-to-end and
 * deterministically (no timing dependence).
 */
class MigrationManagerTest {

    /** Real gateway implementation: records every outbound message, acks synchronously. */
    private static final class RecordingGateway implements MigrationManager.MigrationRpcGateway {
        final List<MigrationMessage> sent = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<MigrationMessage> sendToTaskManager(
                String taskManagerLocation, MigrationMessage message) {
            sent.add(message);
            return CompletableFuture.completedFuture(
                    new MigrationResponse(message.getMigrationId(), true, "ok"));
        }

        boolean sentAnyOfType(Class<? extends MigrationMessage> type) {
            return sent.stream().anyMatch(type::isInstance);
        }
    }

    private static MigrationPlan planForKeyGroup(int keyGroup) {
        return new MigrationPlan(
                keyGroup,
                new OperatorID(),
                new MigrationPlan.OperatorInstance(0, "tm-source"),
                new MigrationPlan.OperatorInstance(1, "tm-target"),
                new JobID(),
                4096L);
    }

    @Test
    void drivesFullLifecycleFromRealEvents() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        MigrationManager manager = new MigrationManager(new JobID(), gateway);
        List<MigrationBarrier> injectedBarriers = new CopyOnWriteArrayList<>();
        manager.setBarrierCallback(injectedBarriers::add);

        try {
            List<Long> ids =
                    manager.startMigrations(List.of(planForKeyGroup(5))).get(5, TimeUnit.SECONDS);
            assertEquals(1, ids.size());
            long id = ids.get(0);
            MigrationContext ctx = manager.getMigration(id);

            // Prepare + StartPreCopy have been dispatched; we are now pre-copying.
            assertEquals(MigrationState.PRE_COPY, ctx.getCurrentState());
            assertTrue(gateway.sentAnyOfType(PrepareMigrationSource.class));
            assertTrue(gateway.sentAnyOfType(PrepareMigrationTarget.class));
            assertTrue(gateway.sentAnyOfType(StartPreCopy.class));

            // A large, not-yet-converged round keeps us in pre-copy.
            manager.handleMessage(new PreCopyRoundComplete(id, 1, 10_000_000L, 10_000_000L, false));
            assertEquals(MigrationState.PRE_COPY, ctx.getCurrentState());

            // A converged round triggers the cutover: barrier injection + AWAIT_BARRIER.
            manager.handleMessage(new PreCopyRoundComplete(id, 2, 1_000L, 1_000L, true));
            assertEquals(MigrationState.AWAIT_BARRIER, ctx.getCurrentState());
            assertEquals(1, injectedBarriers.size());

            // Barrier alignment drives FINALIZING; the final-delta ack drives COMMITTED.
            manager.onBarrierAligned(id);
            assertEquals(MigrationState.COMMITTED, ctx.getCurrentState());
            assertTrue(gateway.sentAnyOfType(TransferFinalDelta.class));
            assertTrue(gateway.sentAnyOfType(CommitMigration.class));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    void abortsWhenDeltaLogFailsToShrink() throws Exception {
        RecordingGateway gateway = new RecordingGateway();
        MigrationManager manager = new MigrationManager(new JobID(), gateway);
        manager.setBarrierCallback(b -> {});

        try {
            long id =
                    manager.startMigrations(List.of(planForKeyGroup(7)))
                            .get(5, TimeUnit.SECONDS)
                            .get(0);
            MigrationContext ctx = manager.getMigration(id);
            assertEquals(MigrationState.PRE_COPY, ctx.getCurrentState());

            // Delta never shrinks: with the default of 3 non-decreasing rounds, the 4th round
            // (3 non-decreasing comparisons) trips the divergence safeguard.
            manager.handleMessage(new PreCopyRoundComplete(id, 1, 5_000_000L, 0L, false));
            manager.handleMessage(new PreCopyRoundComplete(id, 2, 5_000_000L, 0L, false));
            manager.handleMessage(new PreCopyRoundComplete(id, 3, 5_000_000L, 0L, false));
            assertEquals(MigrationState.PRE_COPY, ctx.getCurrentState());
            manager.handleMessage(new PreCopyRoundComplete(id, 4, 5_000_000L, 0L, false));

            assertEquals(MigrationState.ABORTED, ctx.getCurrentState());
            assertTrue(gateway.sentAnyOfType(AbortMigration.class));
        } finally {
            manager.shutdown();
        }
    }
}
