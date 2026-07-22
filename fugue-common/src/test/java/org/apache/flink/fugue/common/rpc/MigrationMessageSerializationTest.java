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

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.fugue.common.rpc.MigrationMessages.AbortMigration;
import org.apache.flink.fugue.common.rpc.MigrationMessages.CommitMigration;
import org.apache.flink.fugue.common.rpc.MigrationMessages.FinalDeltaComplete;
import org.apache.flink.fugue.common.rpc.MigrationMessages.MigrationResponse;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PreCopyRoundComplete;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PrepareMigrationSource;
import org.apache.flink.fugue.common.rpc.MigrationMessages.PrepareMigrationTarget;
import org.apache.flink.fugue.common.rpc.MigrationMessages.StartPreCopy;
import org.apache.flink.fugue.common.rpc.MigrationMessages.TransferFinalDelta;
import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-node foundation: every control message that traverses the coordinator↔controller RPC must be
 * Java-serializable, because real Flink RPC (Pekko over TCP) serializes invocation arguments — whereas
 * the single-JVM {@code TestingRpcService} used in the other ITs passes them by reference and would mask
 * a non-serializable field. This round-trips each message (and a {@link MigrationPlan}) through Java
 * serialization and checks the payload survives, guarding against a field that isn't wire-safe.
 */
class MigrationMessageSerializationTest {

    private static MigrationPlan samplePlan() {
        return new MigrationPlan(
                42,
                new OperatorID(),
                new MigrationPlan.OperatorInstance(0, "pekko.tcp://flink@host-a:6122/user/rpc/fugue-controller"),
                new MigrationPlan.OperatorInstance(2, "pekko.tcp://flink@host-b:6122/user/rpc/fugue-controller"),
                new JobID(),
                4096L);
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T obj) throws Exception {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(obj);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }

    @Test
    void migrationPlanRoundTrips() throws Exception {
        final MigrationPlan original = samplePlan();
        final MigrationPlan copy = roundTrip(original);
        assertEquals(original.getPartitionId(), copy.getPartitionId());
        assertEquals(original.getOperatorId(), copy.getOperatorId());
        assertEquals(original.getSourceInstance(), copy.getSourceInstance());
        assertEquals(original.getTargetInstance(), copy.getTargetInstance());
        assertEquals(original.getJobId(), copy.getJobId());
        assertEquals(original, copy);
    }

    @Test
    void prepareMigrationSourceRoundTrips() throws Exception {
        final PrepareMigrationSource copy =
                roundTrip(new PrepareMigrationSource(1L, samplePlan(), "host-b", 8888));
        assertEquals(1L, copy.getMigrationId());
        assertEquals("host-b", copy.getTargetAddress());
        assertEquals(8888, copy.getTargetPort());
        assertEquals(42, copy.getMigrationPlan().getPartitionId());
    }

    @Test
    void prepareMigrationTargetRoundTrips() throws Exception {
        final PrepareMigrationTarget copy =
                roundTrip(new PrepareMigrationTarget(1L, samplePlan(), 8888));
        assertEquals(8888, copy.getListenPort());
        assertEquals(42, copy.getMigrationPlan().getPartitionId());
    }

    @Test
    void startPreCopyRoundTrips() throws Exception {
        final StartPreCopy copy = roundTrip(new StartPreCopy(1L, 42, 100L * 1024 * 1024));
        assertEquals(42, copy.getPartitionId());
        assertEquals(100L * 1024 * 1024, copy.getRateLimitBytesPerSec());
    }

    @Test
    void preCopyRoundCompleteRoundTrips() throws Exception {
        final PreCopyRoundComplete copy = roundTrip(new PreCopyRoundComplete(1L, 3, 0L, 2048L, true));
        assertEquals(3, copy.getRoundNumber());
        assertEquals(0L, copy.getDeltaSize());
        assertEquals(2048L, copy.getBytesTransferred());
        assertTrue(copy.isConverged());
    }

    @Test
    void transferFinalDeltaAndCompleteRoundTrip() throws Exception {
        assertEquals(42, roundTrip(new TransferFinalDelta(1L, 42)).getPartitionId());
        final FinalDeltaComplete fdc = roundTrip(new FinalDeltaComplete(1L, 42, 512L));
        assertEquals(512L, fdc.getFinalDeltaSize());
    }

    @Test
    void injectBarrierRoundTrips() throws Exception {
        final MigrationMessages.InjectBarrier copy =
                roundTrip(
                        new MigrationMessages.InjectBarrier(
                                99L, java.util.Collections.singletonList(samplePlan()), "fugue-source", false));
        assertEquals(99L, copy.getMigrationId());
        assertEquals("fugue-source", copy.getSourceNamePrefix());
        assertEquals(1, copy.getPlans().size());
        assertEquals(42, copy.getPlans().get(0).getPartitionId());
    }

    @Test
    void commitAbortResponseRoundTrip() throws Exception {
        assertEquals(42, roundTrip(new CommitMigration(1L, 42)).getPartitionId());
        assertEquals("R_dirty>=R_net", roundTrip(new AbortMigration(1L, "R_dirty>=R_net")).getReason());
        final MigrationResponse resp = roundTrip(new MigrationResponse(1L, true, "ok"));
        assertTrue(resp.isSuccess());
        assertEquals("ok", resp.getMessage());
    }
}
