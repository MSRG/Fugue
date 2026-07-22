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

package org.apache.flink.fugue.runtime.transfer;

import org.apache.flink.contrib.streaming.state.RocksDBBackendAccessor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.fugue.runtime.operator.FugueCutover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.util.List;

/**
 * The multi-TaskManager {@link StateMigrationService}: it performs the cutover against a single
 * <em>local</em> operator backend, moving the key-group's state to/from the peer node over a dedicated
 * TCP connection ({@link NetworkedKeyGroupTransfer}). One instance lives on each node and
 * plays whichever role the coordinator asks of it:
 *
 * <ul>
 *   <li><b>Target</b> ({@link #prepareTarget}): host the migrating key-group on the local backend
 *       (cross-range hosting), arm the operator's post-barrier buffer, and start a listener that ingests
 *       each streamed round; on the final round it releases the buffer ({@code requestReplay}).
 *   <li><b>Source</b> ({@link #prepareSource} + {@link #transferSnapshot}/{@link #transferFinalDelta}):
 *       extract the local key-group and stream it to the target's {@code (address, port)}.
 * </ul>
 *
 * <p>It is patched-only (it calls {@code addHostedKeyGroup}) and operator-level (it touches
 * {@code RocksDBKeyedStateBackend}), so it is excluded from the clean build and dropped from the
 * flink-runtime vendoring — the per-node operator code supplies it to the controller via the
 * {@code StateMigrationService} seam (see the multi-node deployment guide). The single-node in-JVM
 * equivalent is {@link OperatorStateMigrationService}.
 *
 * <p>This minimal form assumes one keyed-operator subtask per TaskManager (the local backend is
 * unambiguous); supporting several subtasks per TM means threading the target subtask index through.
 */
public class NetworkedStateMigrationService implements StateMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkedStateMigrationService.class);

    private final RocksDBKeyedStateBackend<?> localBackend;
    private final int localSubtask;
    private final List<String> stateNames;

    private volatile int keyGroup = -1;
    private volatile String targetAddress;
    private volatile int targetPort;

    private volatile ServerSocket serverSocket;
    private volatile Thread receiver;
    private volatile boolean running;

    public NetworkedStateMigrationService(
            RocksDBKeyedStateBackend<?> localBackend, int localSubtask, List<String> stateNames) {
        this.localBackend = localBackend;
        this.localSubtask = localSubtask;
        this.stateNames = stateNames;
    }

    /** The port the target is listening on (after {@link #prepareTarget}); useful when bound to 0. */
    public int getListenPort() {
        final ServerSocket s = serverSocket;
        return s == null ? -1 : s.getLocalPort();
    }

    @Override
    public void prepareSource(long migrationId, int partitionId, String targetAddress, int targetPort) {
        this.keyGroup = partitionId;
        this.targetAddress = targetAddress;
        this.targetPort = targetPort;
    }

    @Override
    public void prepareTarget(long migrationId, int partitionId, int listenPort) throws Exception {
        this.keyGroup = partitionId;
        // Cross-range hosting + post-barrier buffering for the migrating key-group on this node.
        localBackend.addHostedKeyGroup(partitionId);
        FugueCutover.armBuffering(localSubtask, partitionId);
        // Listen for the streamed state and ingest each round into the local backend.
        this.serverSocket = NetworkedKeyGroupTransfer.listen(listenPort);
        this.running = true;
        this.receiver =
                new Thread(() -> receiveRounds(partitionId), "fugue-state-receiver-mig-" + migrationId);
        this.receiver.setDaemon(true);
        this.receiver.start();
    }

    private void receiveRounds(int partitionId) {
        try {
            while (running) {
                final boolean finalRound =
                        NetworkedKeyGroupTransfer.receiveAndIngest(serverSocket, localBackend);
                if (finalRound) {
                    // The final delta has landed: release O_new's buffered post-barrier records.
                    FugueCutover.requestReplay(localSubtask, partitionId);
                    return;
                }
            }
        } catch (Exception e) {
            if (running) {
                LOG.warn("Migration state receiver for key-group {} failed", partitionId, e);
            }
        }
    }

    @Override
    public StateMigrationService.TransferResult transferSnapshot(
            long migrationId, int partitionId, int roundNumber, long rateLimitBytesPerSec)
            throws Exception {
        // Source side: stream the local key-group to the target (bulk pre-copy; converges in one round
        // — the iterative byte-minimal delta is an optimization on top, see the deployment guide).
        final long bytes =
                NetworkedKeyGroupTransfer.send(
                        localBackend, stateNames, partitionId, targetAddress, targetPort, false);
        return new StateMigrationService.TransferResult(bytes, 0L);
    }

    @Override
    public StateMigrationService.TransferResult transferFinalDelta(long migrationId, int partitionId)
            throws Exception {
        // Source side: stream the final state; the target ingests it and releases its buffer.
        final long bytes =
                NetworkedKeyGroupTransfer.send(
                        localBackend, stateNames, partitionId, targetAddress, targetPort, true);
        return new StateMigrationService.TransferResult(bytes, 0L);
    }

    @Override
    public void stopDeltaLogging(long migrationId) {}

    @Override
    public void deletePartitionState(long migrationId, int partitionId) {
        // On commit the source may drop the migrated key-group; left to the source operator's lifecycle.
    }

    @Override
    public void abortSource(long migrationId) {
        // O_old retains its authoritative state.
    }

    @Override
    public void abortTarget(long migrationId) {
        running = false;
        closeServer();
        if (keyGroup >= 0) {
            try {
                KeyGroupStateTransfer.deleteKeyGroup(
                        RocksDBBackendAccessor.db(localBackend),
                        RocksDBBackendAccessor.columnFamily(localBackend, stateNames.get(0)),
                        keyGroup,
                        RocksDBBackendAccessor.keyGroupPrefixBytes(localBackend));
            } catch (Exception e) {
                LOG.warn("Failed to roll back hosted key-group {} on abort", keyGroup, e);
            }
            FugueCutover.finishAll(localSubtask);
        }
    }

    @Override
    public void shutdown() {
        running = false;
        closeServer();
    }

    private void closeServer() {
        final ServerSocket s = serverSocket;
        if (s != null) {
            try {
                s.close();
            } catch (Exception ignored) {
                // closing to interrupt the blocking accept()
            }
        }
    }
}
