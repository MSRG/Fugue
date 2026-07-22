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
import org.apache.flink.fugue.coordinator.manager.MigrationManager;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * RPC Gateway implementation for Fugue migration coordination.
 * Integrates with Flink's Akka RPC framework for message passing.
 */
public class FugueRpcGateway implements MigrationManager.MigrationRpcGateway {
    private static final Logger LOG = LoggerFactory.getLogger(FugueRpcGateway.class);

    /** Flink's RPC service. */
    private final RpcService rpcService;

    /** Cache of TaskManager RPC endpoints. */
    private final Map<String, TaskManagerMigrationGateway> taskManagerGateways;

    /** Timeout for RPC calls (milliseconds). */
    private final long rpcTimeout;

    /** Retry attempts for failed RPCs. */
    private final int maxRetries;

    public FugueRpcGateway(RpcService rpcService) {
        this(rpcService, 30000L, 3);
    }

    public FugueRpcGateway(RpcService rpcService, long rpcTimeout, int maxRetries) {
        this.rpcService = rpcService;
        this.taskManagerGateways = new ConcurrentHashMap<>();
        this.rpcTimeout = rpcTimeout;
        this.maxRetries = maxRetries;
    }

    @Override
    public CompletableFuture<MigrationMessages.MigrationMessage> sendToTaskManager(
            String taskManagerLocation,
            MigrationMessages.MigrationMessage message) {

        return getOrConnectTaskManager(taskManagerLocation)
                .thenCompose(gateway -> sendWithRetry(gateway, message, 0));
    }

    /**
     * Send message with retry logic.
     */
    private CompletableFuture<MigrationMessages.MigrationMessage> sendWithRetry(
            TaskManagerMigrationGateway gateway,
            MigrationMessages.MigrationMessage message,
            int attemptNumber) {

        LOG.debug("Sending {} to TaskManager (attempt {})",
                message.getClass().getSimpleName(), attemptNumber + 1);

        return gateway.handleMigrationMessage(message)
                .exceptionally(throwable -> {
                    LOG.warn("RPC failed (attempt {}): {}",
                            attemptNumber + 1, throwable.getMessage());

                    if (attemptNumber < maxRetries - 1) {
                        // Retry after delay
                        try {
                            Thread.sleep(1000 * (attemptNumber + 1)); // Exponential backoff
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return sendWithRetry(gateway, message, attemptNumber + 1).join();
                    } else {
                        LOG.error("RPC failed after {} attempts", maxRetries, throwable);
                        return new MigrationMessages.MigrationResponse(
                                message.getMigrationId(),
                                false,
                                "RPC failed: " + throwable.getMessage());
                    }
                });
    }

    /**
     * Get or create connection to TaskManager.
     */
    private CompletableFuture<TaskManagerMigrationGateway> getOrConnectTaskManager(
            String taskManagerLocation) {

        TaskManagerMigrationGateway cached = taskManagerGateways.get(taskManagerLocation);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        // Connect to TaskManager
        return rpcService.connect(
                taskManagerLocation,
                TaskManagerMigrationGateway.class)
                .thenApply(gateway -> {
                    taskManagerGateways.put(taskManagerLocation, gateway);
                    LOG.info("Connected to TaskManager at {}", taskManagerLocation);
                    return gateway;
                });
    }

    /**
     * Clear cached connections (e.g., on TaskManager failure).
     */
    public void clearConnection(String taskManagerLocation) {
        taskManagerGateways.remove(taskManagerLocation);
        LOG.info("Cleared connection to TaskManager at {}", taskManagerLocation);
    }

    /**
     * Close all connections.
     */
    public void close() {
        taskManagerGateways.clear();
        LOG.info("Closed all TaskManager connections");
    }

    /**
     * Gateway interface for TaskManager-side migration handling.
     * This interface should be implemented by TaskManager's RPC endpoint.
     */
    public interface TaskManagerMigrationGateway extends RpcGateway {

        /**
         * Handle migration message from coordinator.
         *
         * @param message The migration message
         * @return Response future
         */
        CompletableFuture<MigrationMessages.MigrationMessage> handleMigrationMessage(
                MigrationMessages.MigrationMessage message);
    }
}