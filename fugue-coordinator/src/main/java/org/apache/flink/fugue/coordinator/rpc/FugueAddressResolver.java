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

/**
 * Resolves the RPC address of a TaskManager's Fugue migration controller from the TaskManager's own
 * (TaskExecutor) RPC address — the address-discovery strategy for the multi-TaskManager deployment.
 *
 * <p>On a real cluster the coordinator (in the JobMaster) learns, from the {@code ExecutionGraph} +
 * the JobMaster's registered TaskManagers, which TaskManager hosts a given operator subtask and that
 * TaskManager's TaskExecutor RPC address (e.g.
 * {@code pekko.tcp://flink@host:6122/user/rpc/taskmanager_0}). The Fugue controller
 * ({@code FugueTaskExecutorService}) runs as a separate endpoint on the <em>same</em> RPC actor system,
 * so its address is the same authority with the endpoint path swapped — this class performs that swap.
 *
 * <p>For this to be derivable, a real-cluster controller must use the well-known endpoint id
 * {@link #CONTROLLER_ENDPOINT_NAME} (the multi-TM deployment guide covers this; it is intentionally
 * <em>not</em> hard-wired into {@code FugueTaskExecutorService}, because the single-JVM tests run several
 * controllers on one RPC service, where a fixed id would collide). The alternative is controller→coordinator
 * self-registration, also described in the guide.
 */
public final class FugueAddressResolver {

    /** Well-known endpoint id a controller uses on a real cluster so its address is derivable. */
    public static final String CONTROLLER_ENDPOINT_NAME = "fugue-controller";

    private FugueAddressResolver() {}

    /**
     * Derive the Fugue controller's RPC address co-located with the given TaskExecutor RPC address, by
     * replacing the trailing endpoint-path segment with {@link #CONTROLLER_ENDPOINT_NAME}. Returns the
     * input unchanged if it has no path segment to swap.
     */
    public static String controllerAddressFrom(String taskExecutorRpcAddress) {
        if (taskExecutorRpcAddress == null) {
            return null;
        }
        final int lastSlash = taskExecutorRpcAddress.lastIndexOf('/');
        if (lastSlash < 0 || lastSlash == taskExecutorRpcAddress.length() - 1) {
            return taskExecutorRpcAddress;
        }
        return taskExecutorRpcAddress.substring(0, lastSlash + 1) + CONTROLLER_ENDPOINT_NAME;
    }
}
