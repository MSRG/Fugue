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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link StateMigrationService} that moves a key-group between two <em>live</em> operator
 * {@link RocksDBKeyedStateBackend}s using the SST bulk-load path (the initial background
 * copy): per registered state, export the key-group to an SST via
 * {@link KeyGroupStateTransfer#exportKeyGroupToSst} and bulk-load it into the target via
 * {@link KeyGroupStateTransfer#ingestSst}. Backends are reached through {@link RocksDBBackendAccessor}.
 *
 * <p>On a single-JVM MiniCluster both backends are local, so there is no TCP hop (the
 * cross-TaskManager transport is handled by {@link NetworkedStateMigrationService}). This service
 * assumes the migrating key-group is
 * quiescent during the copy, which yields the no-delta case
 * ({@code S_new(P_k) = S(P_k, t1) = S(P_k, t2)}).
 */
public class OperatorStateMigrationService implements StateMigrationService {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorStateMigrationService.class);

    private final RocksDBKeyedStateBackend<?> sourceBackend;
    private final RocksDBKeyedStateBackend<?> targetBackend;
    private final List<String> stateNames;
    private final Path tmpDir;

    /** Key-group being migrated (set on the first transfer round); used for target-side abort rollback. */
    private volatile int migratingKeyGroup = -1;

    public OperatorStateMigrationService(
            RocksDBKeyedStateBackend<?> sourceBackend,
            RocksDBKeyedStateBackend<?> targetBackend,
            List<String> stateNames)
            throws IOException {
        this.sourceBackend = sourceBackend;
        this.targetBackend = targetBackend;
        this.stateNames = stateNames;
        this.tmpDir = Files.createTempDirectory("fugue-sst-");
    }

    @Override
    public void prepareSource(long migrationId, int partitionId, String targetAddress, int targetPort) {}

    @Override
    public void prepareTarget(long migrationId, int partitionId, int listenPort) {}

    @Override
    public StateMigrationService.TransferResult transferSnapshot(
            long migrationId, int partitionId, int roundNumber, long rateLimitBytesPerSec)
            throws Exception {
        final int keyGroup = partitionId; // MigrationPlan.partitionId == key-group.
        this.migratingKeyGroup = keyGroup;
        final RocksDB srcDb = RocksDBBackendAccessor.db(sourceBackend);
        final RocksDB tgtDb = RocksDBBackendAccessor.db(targetBackend);
        final int prefixBytes = RocksDBBackendAccessor.keyGroupPrefixBytes(sourceBackend);

        if (roundNumber > 1) {
            // Iterative delta round: apply the net change needed for the key-group,
            // as key/value pairs through the normal write path.
            long changed = 0;
            for (String stateName : stateNames) {
                final ColumnFamilyHandle srcCf =
                        RocksDBBackendAccessor.columnFamily(sourceBackend, stateName);
                final ColumnFamilyHandle tgtCf =
                        RocksDBBackendAccessor.columnFamily(targetBackend, stateName);
                changed +=
                        KeyGroupStateTransfer.applyKeyGroupDelta(
                                srcDb, srcCf, tgtDb, tgtCf, keyGroup, prefixBytes);
            }
            LOG.info(
                    "Migration {}: delta round {} for key-group {} applied {} changed entries",
                    migrationId, roundNumber, keyGroup, changed);
            // deltaSize = number of changed entries -> drives the convergence/divergence check.
            return new StateMigrationService.TransferResult(changed, changed);
        }

        long bytesMoved = 0;
        for (String stateName : stateNames) {
            final ColumnFamilyHandle srcCf = RocksDBBackendAccessor.columnFamily(sourceBackend, stateName);
            final ColumnFamilyHandle tgtCf = RocksDBBackendAccessor.columnFamily(targetBackend, stateName);
            final Path sst =
                    tmpDir.resolve("mig-" + migrationId + "-kg-" + keyGroup + "-" + stateName + ".sst");

            final long entries =
                    KeyGroupStateTransfer.exportKeyGroupToSst(srcDb, srcCf, keyGroup, prefixBytes, sst);
            if (entries > 0) {
                KeyGroupStateTransfer.ingestSst(tgtDb, tgtCf, sst);
                bytesMoved += Files.size(sst);
                LOG.info(
                        "Migration {}: bulk-copied key-group {} state '{}' ({} entries) via SST ingest",
                        migrationId, keyGroup, stateName, entries);
            }
        }
        // No live writes here -> deltaSize 0 (immediate convergence).
        return new StateMigrationService.TransferResult(bytesMoved, 0L);
    }

    @Override
    public StateMigrationService.TransferResult transferFinalDelta(long migrationId, int partitionId) {
        return new StateMigrationService.TransferResult(0L, 0L);
    }

    @Override
    public void stopDeltaLogging(long migrationId) {}

    @Override
    public void deletePartitionState(long migrationId, int partitionId) {}

    @Override
    public void abortSource(long migrationId) {
        // O_old retains its authoritative state and simply ceases the migration; the snapshot-diff
        // delta mechanism keeps no separate delta log to discard.
        LOG.info("Migration {}: aborted at source for key-group {} (source retains ownership)",
                migrationId, migratingKeyGroup);
    }

    @Override
    public void abortTarget(long migrationId) {
        // O_new deletes any partially-received state for the migrating key-group.
        if (migratingKeyGroup < 0) {
            return;
        }
        try {
            final RocksDB tgtDb = RocksDBBackendAccessor.db(targetBackend);
            final int prefixBytes = RocksDBBackendAccessor.keyGroupPrefixBytes(targetBackend);
            for (String stateName : stateNames) {
                final ColumnFamilyHandle tgtCf =
                        RocksDBBackendAccessor.columnFamily(targetBackend, stateName);
                final long deleted =
                        KeyGroupStateTransfer.deleteKeyGroup(tgtDb, tgtCf, migratingKeyGroup, prefixBytes);
                LOG.info(
                        "Migration {}: aborted - discarded {} partially-received entries of key-group {} "
                                + "state '{}' at target",
                        migrationId, deleted, migratingKeyGroup, stateName);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Migration {}: failed to roll back partially-received state for key-group {}",
                    migrationId, migratingKeyGroup, e);
        }
    }

    @Override
    public void shutdown() {
        try {
            if (Files.exists(tmpDir)) {
                Files.walk(tmpDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        Files.delete(p);
                                    } catch (IOException e) {
                                        LOG.warn("Failed to delete {}", p, e);
                                    }
                                });
            }
        } catch (IOException e) {
            LOG.warn("Failed to clean up temp SST dir {}", tmpDir, e);
        }
    }
}
