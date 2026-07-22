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

/**
 * Abstraction over the mechanism that physically moves a partition's (key-group's) state from a
 * source to a target operator instance during a migration. The {@code MigrationController}
 * orchestrates the protocol against this interface, so the state plane is pluggable and lives at the
 * operator level (where the live RocksDB keyed backend is reachable), decoupled from the control
 * plane. Implementations:
 *
 * <ul>
 *   <li>{@link OperatorStateMigrationService} — moves one key-group between two <em>live</em> operator
 *       {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend}s in-JVM: an initial
 *       SST bulk copy ({@code SstFileWriter} + {@code IngestExternalFile}) plus iterative
 *       snapshot-diff deltas through the normal write path until convergence.
 *   <li>{@link NetworkedStateMigrationService} — the same cutover across nodes, streaming the
 *       key-group over a dedicated TCP socket.
 *   <li>tests — an in-memory implementation that exercises the control-plane protocol
 *       deterministically without sockets or RocksDB.
 * </ul>
 */
public interface StateMigrationService {

    /** Prepare the source instance: snapshot the partition and get ready to stream it. */
    void prepareSource(long migrationId, int partitionId, String targetAddress, int targetPort)
            throws Exception;

    /** Prepare the target instance: get ready to receive the partition's state. */
    void prepareTarget(long migrationId, int partitionId, int listenPort) throws Exception;

    /**
     * Run one pre-copy round (initial bulk transfer or an iterative delta). {@code rateLimitBytesPerSec}
     * is an advisory bandwidth hint; the in-repo transfers move the key-group in full per round and do
     * not currently throttle by it.
     */
    TransferResult transferSnapshot(
            long migrationId, int partitionId, int roundNumber, long rateLimitBytesPerSec)
            throws Exception;

    /** Transfer the final delta after barrier alignment. */
    TransferResult transferFinalDelta(long migrationId, int partitionId) throws Exception;

    /** Stop capturing further modifications for the migrating partition. */
    void stopDeltaLogging(long migrationId);

    /** Delete the migrated partition's state on the source after a successful commit. */
    void deletePartitionState(long migrationId, int partitionId);

    /** Abort and clean up source-side resources for a migration. */
    void abortSource(long migrationId);

    /** Abort and clean up target-side resources for a migration. */
    void abortTarget(long migrationId);

    /** Release all resources held by the service. */
    void shutdown();

    /** The outcome of one transfer round: how many bytes moved and the residual delta size. */
    class TransferResult {
        private final long bytesTransferred;
        private final long deltaSize;

        public TransferResult(long bytesTransferred, long deltaSize) {
            this.bytesTransferred = bytesTransferred;
            this.deltaSize = deltaSize;
        }

        public long getBytesTransferred() {
            return bytesTransferred;
        }

        public long getDeltaSize() {
            return deltaSize;
        }
    }
}
