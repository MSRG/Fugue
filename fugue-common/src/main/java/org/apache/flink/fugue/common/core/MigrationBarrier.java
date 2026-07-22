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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Migration barrier that flows in-band with data records through the dataflow graph, carrying the
 * migration plan(s) for an atomic cutover.
 *
 * <p>It is a Flink {@link RuntimeEvent}: Flink's {@code EventSerializer} serializes any unknown
 * {@link org.apache.flink.runtime.event.AbstractEvent} subclass through its generic {@code OTHER_EVENT}
 * path (class name + {@link #write}), and reconstructs it with {@code InstantiationUtil.instantiate}
 * + {@link #read}. So this barrier traverses the real network stack <em>without</em> extending
 * {@code CheckpointBarrier} or registering a bespoke serializer. The {@linkplain #MigrationBarrier()
 * public no-arg constructor} is required for that deserialization path.
 */
public class MigrationBarrier extends RuntimeEvent {

    /** Unique identifier for this migration round. */
    private long migrationId;

    /** Whether this is the final barrier for the migration. */
    private boolean finalBarrier;

    /** Migration plans carried by this barrier. */
    private List<MigrationPlan> migrationPlans;

    /** Required for {@code EventSerializer} {@code OTHER_EVENT} deserialization. */
    public MigrationBarrier() {
        this.migrationPlans = new ArrayList<>();
    }

    public MigrationBarrier(long migrationId, List<MigrationPlan> migrationPlans, boolean finalBarrier) {
        this.migrationId = migrationId;
        this.migrationPlans = new ArrayList<>(migrationPlans);
        this.finalBarrier = finalBarrier;
    }

    /** A migration barrier carrying the plans to cut over. */
    public static MigrationBarrier createStandalone(long migrationId, List<MigrationPlan> migrationPlans) {
        return new MigrationBarrier(migrationId, migrationPlans, false);
    }

    /** A final barrier that signals migration completion (carries no plans). */
    public static MigrationBarrier createFinalBarrier(long migrationId) {
        return new MigrationBarrier(migrationId, Collections.emptyList(), true);
    }

    public List<MigrationPlan> getMigrationPlans() {
        return Collections.unmodifiableList(migrationPlans);
    }

    public long getMigrationId() {
        return migrationId;
    }

    public boolean isFinalBarrier() {
        return finalBarrier;
    }

    /** Whether this barrier affects a specific partition (key-group). */
    public boolean affectsPartition(int partitionId) {
        return migrationPlans.stream().anyMatch(plan -> plan.getPartitionId() == partitionId);
    }

    /** The migration plan for a specific partition (key-group), if any. */
    public MigrationPlan getMigrationPlanForPartition(int partitionId) {
        return migrationPlans.stream()
                .filter(plan -> plan.getPartitionId() == partitionId)
                .findFirst()
                .orElse(null);
    }

    // ------------------------------------------------------------------------
    //  Flink network-stack (de)serialization — generic OTHER_EVENT path
    // ------------------------------------------------------------------------

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeLong(migrationId);
        out.writeBoolean(finalBarrier);
        out.writeInt(migrationPlans.size());
        for (MigrationPlan plan : migrationPlans) {
            writePlan(plan, out);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.migrationId = in.readLong();
        this.finalBarrier = in.readBoolean();
        final int size = in.readInt();
        this.migrationPlans = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            migrationPlans.add(readPlan(in));
        }
    }

    private static void writePlan(MigrationPlan plan, DataOutputView out) throws IOException {
        out.writeInt(plan.getPartitionId());
        final byte[] operatorId = plan.getOperatorId().getBytes();
        out.writeInt(operatorId.length);
        out.write(operatorId);
        writeInstance(plan.getSourceInstance(), out);
        writeInstance(plan.getTargetInstance(), out);
        final byte[] jobId = plan.getJobId().getBytes();
        out.writeInt(jobId.length);
        out.write(jobId);
        out.writeLong(plan.getEstimatedStateSize());
    }

    private static MigrationPlan readPlan(DataInputView in) throws IOException {
        final int partitionId = in.readInt();
        final byte[] operatorId = new byte[in.readInt()];
        in.readFully(operatorId);
        final MigrationPlan.OperatorInstance source = readInstance(in);
        final MigrationPlan.OperatorInstance target = readInstance(in);
        final byte[] jobId = new byte[in.readInt()];
        in.readFully(jobId);
        final long estimatedStateSize = in.readLong();
        return new MigrationPlan(
                partitionId, new OperatorID(operatorId), source, target, new JobID(jobId), estimatedStateSize);
    }

    private static void writeInstance(MigrationPlan.OperatorInstance instance, DataOutputView out)
            throws IOException {
        out.writeInt(instance.getSubtaskIndex());
        out.writeUTF(instance.getTaskManagerLocation());
    }

    private static MigrationPlan.OperatorInstance readInstance(DataInputView in) throws IOException {
        final int subtaskIndex = in.readInt();
        final String taskManagerLocation = in.readUTF();
        return new MigrationPlan.OperatorInstance(subtaskIndex, taskManagerLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MigrationBarrier that = (MigrationBarrier) o;
        return migrationId == that.migrationId
                && finalBarrier == that.finalBarrier
                && Objects.equals(migrationPlans, that.migrationPlans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(migrationId, finalBarrier, migrationPlans);
    }

    @Override
    public String toString() {
        return String.format(
                "MigrationBarrier{migrationId=%d, plans=%d, final=%b}",
                migrationId, migrationPlans.size(), finalBarrier);
    }
}
