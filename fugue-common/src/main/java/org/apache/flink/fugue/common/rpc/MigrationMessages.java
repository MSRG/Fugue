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

package org.apache.flink.fugue.common.rpc;

import org.apache.flink.fugue.common.core.MigrationPlan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * RPC messages for migration coordination between JobManager and TaskManagers.
 */
public class MigrationMessages {

    /**
     * Base class for all migration RPC messages.
     */
    public abstract static class MigrationMessage implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final long migrationId;
        protected final long timestamp;

        public MigrationMessage(long migrationId) {
            this.migrationId = migrationId;
            this.timestamp = System.currentTimeMillis();
        }

        public long getMigrationId() {
            return migrationId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Request to prepare source operator for migration.
     */
    public static class PrepareMigrationSource extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final MigrationPlan migrationPlan;
        private final String targetAddress;
        private final int targetPort;

        public PrepareMigrationSource(
                long migrationId,
                MigrationPlan migrationPlan,
                String targetAddress,
                int targetPort) {
            super(migrationId);
            this.migrationPlan = migrationPlan;
            this.targetAddress = targetAddress;
            this.targetPort = targetPort;
        }

        public MigrationPlan getMigrationPlan() {
            return migrationPlan;
        }

        public String getTargetAddress() {
            return targetAddress;
        }

        public int getTargetPort() {
            return targetPort;
        }
    }

    /**
     * Request to prepare target operator for migration.
     */
    public static class PrepareMigrationTarget extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final MigrationPlan migrationPlan;
        private final int listenPort;

        public PrepareMigrationTarget(
                long migrationId,
                MigrationPlan migrationPlan,
                int listenPort) {
            super(migrationId);
            this.migrationPlan = migrationPlan;
            this.listenPort = listenPort;
        }

        public MigrationPlan getMigrationPlan() {
            return migrationPlan;
        }

        public int getListenPort() {
            return listenPort;
        }
    }

    /**
     * Request to start pre-copy phase.
     */
    public static class StartPreCopy extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;
        private final long rateLimitBytesPerSec;

        public StartPreCopy(long migrationId, int partitionId, long rateLimitBytesPerSec) {
            super(migrationId);
            this.partitionId = partitionId;
            this.rateLimitBytesPerSec = rateLimitBytesPerSec;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public long getRateLimitBytesPerSec() {
            return rateLimitBytesPerSec;
        }
    }

    /**
     * Notification that pre-copy round completed.
     */
    public static class PreCopyRoundComplete extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int roundNumber;
        private final long deltaSize;
        private final long bytesTransferred;
        private final boolean converged;

        public PreCopyRoundComplete(
                long migrationId,
                int roundNumber,
                long deltaSize,
                long bytesTransferred,
                boolean converged) {
            super(migrationId);
            this.roundNumber = roundNumber;
            this.deltaSize = deltaSize;
            this.bytesTransferred = bytesTransferred;
            this.converged = converged;
        }

        public int getRoundNumber() {
            return roundNumber;
        }

        public long getDeltaSize() {
            return deltaSize;
        }

        public long getBytesTransferred() {
            return bytesTransferred;
        }

        public boolean isConverged() {
            return converged;
        }
    }

    /**
     * Request to transfer final delta after barrier alignment.
     */
    public static class TransferFinalDelta extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;

        public TransferFinalDelta(long migrationId, int partitionId) {
            super(migrationId);
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }
    }

    /**
     * Notification that final delta transfer completed.
     */
    public static class FinalDeltaComplete extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;
        private final long finalDeltaSize;

        public FinalDeltaComplete(long migrationId, int partitionId, long finalDeltaSize) {
            super(migrationId);
            this.partitionId = partitionId;
            this.finalDeltaSize = finalDeltaSize;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public long getFinalDeltaSize() {
            return finalDeltaSize;
        }
    }

    /**
     * Request to abort migration.
     */
    public static class AbortMigration extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final String reason;

        public AbortMigration(long migrationId, String reason) {
            super(migrationId);
            this.reason = reason;
        }

        public String getReason() {
            return reason;
        }
    }

    /**
     * Request to commit migration.
     */
    public static class CommitMigration extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final int partitionId;

        public CommitMigration(long migrationId, int partitionId) {
            super(migrationId);
            this.partitionId = partitionId;
        }

        public int getPartitionId() {
            return partitionId;
        }
    }

    /**
     * Response message for RPC calls.
     */
    public static class MigrationResponse extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final boolean success;
        private final String message;

        public MigrationResponse(long migrationId, boolean success, String message) {
            super(migrationId);
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * Coordinator → controller command to inject the in-band migration barrier at the local source
     * operators (the cross-node form of barrier injection: in a real cluster the coordinator sends this
     * to each TaskManager hosting a source subtask, and the controller injects into its <em>local</em>
     * sources via {@code FugueStreamTaskRegistry}, which flips that node's routing override in-band).
     */
    public static class InjectBarrier extends MigrationMessage {
        private static final long serialVersionUID = 1L;

        private final List<MigrationPlan> plans;
        private final String sourceNamePrefix;
        private final boolean finalBarrier;

        public InjectBarrier(
                long migrationId,
                List<MigrationPlan> plans,
                String sourceNamePrefix,
                boolean finalBarrier) {
            super(migrationId);
            this.plans = new ArrayList<>(plans);
            this.sourceNamePrefix = sourceNamePrefix;
            this.finalBarrier = finalBarrier;
        }

        public List<MigrationPlan> getPlans() {
            return plans;
        }

        public String getSourceNamePrefix() {
            return sourceNamePrefix;
        }

        public boolean isFinalBarrier() {
            return finalBarrier;
        }
    }

    // Private constructor to prevent instantiation
    private MigrationMessages() {}
}