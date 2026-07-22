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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link MigrationBarrier} traverses Flink's real network stack as a {@link RuntimeEvent}:
 * it must round-trip through {@code EventSerializer}'s generic {@code OTHER_EVENT} path (class name +
 * {@code write}/{@code read} + {@code InstantiationUtil}), with no {@code CheckpointBarrier} subtype and
 * no bespoke serializer. This is the wire-readiness check for the in-band migration barrier.
 */
class MigrationBarrierEventTest {

    @Test
    void roundTripsThroughEventSerializerGenericPath() throws Exception {
        final OperatorID operatorId = new OperatorID();
        final JobID jobId = new JobID();
        final MigrationPlan plan1 =
                new MigrationPlan(
                        7,
                        operatorId,
                        new MigrationPlan.OperatorInstance(0, "tm-a:1"),
                        new MigrationPlan.OperatorInstance(1, "tm-b:2"),
                        jobId,
                        4096L);
        final MigrationPlan plan2 =
                new MigrationPlan(
                        9,
                        operatorId,
                        new MigrationPlan.OperatorInstance(2, "tm-c:3"),
                        new MigrationPlan.OperatorInstance(3, "tm-d:4"),
                        jobId,
                        8192L);
        final MigrationBarrier barrier =
                MigrationBarrier.createStandalone(42L, Arrays.asList(plan1, plan2));

        final ByteBuffer serialized = EventSerializer.toSerializedEvent(barrier);
        final AbstractEvent deserialized =
                EventSerializer.fromSerializedEvent(serialized, getClass().getClassLoader());

        final MigrationBarrier result =
                assertInstanceOf(
                        MigrationBarrier.class,
                        deserialized,
                        "must deserialize via the generic OTHER_EVENT path as a MigrationBarrier");
        assertEquals(42L, result.getMigrationId());
        assertFalse(result.isFinalBarrier());
        assertEquals(2, result.getMigrationPlans().size());

        final MigrationPlan r1 = result.getMigrationPlans().get(0);
        assertEquals(7, r1.getPartitionId());
        assertEquals(operatorId, r1.getOperatorId());
        assertEquals(0, r1.getSourceInstance().getSubtaskIndex());
        assertEquals("tm-a:1", r1.getSourceInstance().getTaskManagerLocation());
        assertEquals(1, r1.getTargetInstance().getSubtaskIndex());
        assertEquals("tm-b:2", r1.getTargetInstance().getTaskManagerLocation());
        assertEquals(jobId, r1.getJobId());
        assertEquals(4096L, r1.getEstimatedStateSize());

        final MigrationPlan r2 = result.getMigrationPlans().get(1);
        assertEquals(9, r2.getPartitionId());
        assertEquals("tm-c:3", r2.getSourceInstance().getTaskManagerLocation());
        assertEquals(8192L, r2.getEstimatedStateSize());
    }

    @Test
    void finalBarrierRoundTrips() throws Exception {
        final MigrationBarrier barrier = MigrationBarrier.createFinalBarrier(5L);

        final ByteBuffer serialized = EventSerializer.toSerializedEvent(barrier);
        final MigrationBarrier result =
                (MigrationBarrier)
                        EventSerializer.fromSerializedEvent(serialized, getClass().getClassLoader());

        assertEquals(5L, result.getMigrationId());
        assertTrue(result.isFinalBarrier());
        assertTrue(result.getMigrationPlans().isEmpty());
        assertEquals(barrier, result);
    }
}
