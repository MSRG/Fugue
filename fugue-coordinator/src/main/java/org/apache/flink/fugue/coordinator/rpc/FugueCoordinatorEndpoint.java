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

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages;
import org.apache.flink.fugue.coordinator.manager.MigrationManager;
import org.apache.flink.fugue.coordinator.planner.PolicyConfiguration;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinator-side RPC endpoint that hosts the {@link MigrationManager}. In a real deployment this
 * is created and started by the JobMaster (see the integration module). It exposes {@link
 * FugueCoordinatorGateway} so that TaskManager-side controllers can report progress back, and it
 * drives migrations through the {@link MigrationManager}, which in turn talks to the controllers
 * via {@link FugueRpcGateway} (built over the same {@link RpcService}).
 */
public class FugueCoordinatorEndpoint extends RpcEndpoint implements FugueCoordinatorGateway {
    private static final Logger LOG = LoggerFactory.getLogger(FugueCoordinatorEndpoint.class);

    private final MigrationManager migrationManager;

    public FugueCoordinatorEndpoint(
            RpcService rpcService, JobID jobId, PolicyConfiguration configuration) {
        super(rpcService);
        this.migrationManager = new MigrationManager(jobId, new FugueRpcGateway(rpcService));
        this.migrationManager.updateConfiguration(configuration);
        // Barrier injection into the data stream is wired by the runtime integration.
        this.migrationManager.setBarrierCallback(this::injectBarrier);
    }

    /** Trigger migrations for the given plans (invoked by the elasticity planner / JobMaster). */
    public CompletableFuture<List<Long>> startMigrations(List<MigrationPlan> plans) {
        return migrationManager.startMigrations(plans);
    }

    @Override
    public void handleMigrationMessage(MigrationMessages.MigrationMessage message) {
        migrationManager.handleMessage(message);
    }

    @Override
    public void notifyBarrierAligned(long migrationId) {
        migrationManager.onBarrierAligned(migrationId);
    }

    /**
     * Default callback for injecting the in-band migration barrier at the dataflow sources. The
     * coordinator-driven path overrides this with an injector that broadcasts the barrier into the live
     * sources via {@code FugueStreamTaskRegistry}; across nodes the coordinator fans a {@code
     * MigrationMessages.InjectBarrier} RPC out to each source-hosting controller (see
     * docs/fugue-multinode-deployment.md). This base implementation is a no-op.
     */
    protected void injectBarrier(MigrationBarrier barrier) {
        LOG.debug("Barrier injection requested for {} (override this callback to inject; see deployment guide)", barrier);
    }

    public MigrationManager getMigrationManager() {
        return migrationManager;
    }

    @Override
    public CompletableFuture<Void> onStop() {
        migrationManager.shutdown();
        return CompletableFuture.completedFuture(null);
    }
}
