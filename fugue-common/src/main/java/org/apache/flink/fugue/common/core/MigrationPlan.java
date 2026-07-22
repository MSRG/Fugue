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

package org.apache.flink.fugue.common.core;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a migration plan for a single partition from source to target operator instance.
 * This is the basic unit of migration in the Fugue system.
 */
public class MigrationPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The partition ID (key group) to migrate. */
    private final int partitionId;

    /** The operator ID that owns this partition. */
    private final OperatorID operatorId;

    /** The source operator instance (currently owning the partition). */
    private final OperatorInstance sourceInstance;

    /** The target operator instance (destination for the partition). */
    private final OperatorInstance targetInstance;

    /** The job ID this migration belongs to. */
    private final JobID jobId;

    /** Estimated state size for this partition in bytes. */
    private final long estimatedStateSize;

    /** Creation timestamp. */
    private final long creationTimestamp;

    public MigrationPlan(
            int partitionId,
            OperatorID operatorId,
            OperatorInstance sourceInstance,
            OperatorInstance targetInstance,
            JobID jobId,
            long estimatedStateSize) {
        this.partitionId = partitionId;
        this.operatorId = Objects.requireNonNull(operatorId);
        this.sourceInstance = Objects.requireNonNull(sourceInstance);
        this.targetInstance = Objects.requireNonNull(targetInstance);
        this.jobId = Objects.requireNonNull(jobId);
        this.estimatedStateSize = estimatedStateSize;
        this.creationTimestamp = System.currentTimeMillis();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public OperatorID getOperatorId() {
        return operatorId;
    }

    public OperatorInstance getSourceInstance() {
        return sourceInstance;
    }

    public OperatorInstance getTargetInstance() {
        return targetInstance;
    }

    public JobID getJobId() {
        return jobId;
    }

    public long getEstimatedStateSize() {
        return estimatedStateSize;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    /**
     * Represents an operator instance in the dataflow graph.
     *
     * <p>An instance is identified by its operator (see {@link MigrationPlan#getOperatorId()}) and
     * its {@code subtaskIndex}. {@code taskManagerLocation} is the RPC address of the migration
     * controller on the TaskManager hosting the instance; on a multi-node cluster it is resolved from
     * the {@code ExecutionGraph} (see {@code FugueAddressResolver}).
     */
    public static class OperatorInstance implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int subtaskIndex;
        private final String taskManagerLocation;

        public OperatorInstance(int subtaskIndex, String taskManagerLocation) {
            this.subtaskIndex = subtaskIndex;
            this.taskManagerLocation = Objects.requireNonNull(taskManagerLocation);
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }

        public String getTaskManagerLocation() {
            return taskManagerLocation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OperatorInstance that = (OperatorInstance) o;
            return subtaskIndex == that.subtaskIndex &&
                    Objects.equals(taskManagerLocation, that.taskManagerLocation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtaskIndex, taskManagerLocation);
        }

        @Override
        public String toString() {
            return String.format("OperatorInstance{subtask=%d, location=%s}",
                    subtaskIndex, taskManagerLocation);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationPlan that = (MigrationPlan) o;
        return partitionId == that.partitionId &&
                Objects.equals(operatorId, that.operatorId) &&
                Objects.equals(sourceInstance, that.sourceInstance) &&
                Objects.equals(targetInstance, that.targetInstance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, operatorId, sourceInstance, targetInstance);
    }

    @Override
    public String toString() {
        return String.format("MigrationPlan{partition=%d, operator=%s, source=%s, target=%s, size=%d bytes}",
                partitionId, operatorId, sourceInstance, targetInstance, estimatedStateSize);
    }
}