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

package org.apache.flink.fugue.runtime.barrier;

import org.apache.flink.api.common.JobID;
import org.apache.flink.fugue.common.core.MigrationBarrier;
import org.apache.flink.fugue.common.core.MigrationPlan;
import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the {@link FugueRoutingOverrides} routing-flip table (no Flink patch needed). */
class FugueRoutingOverridesTest {

    @BeforeEach
    @AfterEach
    void reset() {
        FugueRoutingOverrides.clear();
    }

    @Test
    void returnsDefaultChannelWhenNoOverrideActive() {
        assertFalse(FugueRoutingOverrides.isActive());
        assertEquals(3, FugueRoutingOverrides.channelFor(42, 3));
        assertNull(FugueRoutingOverrides.getOverride(42));
    }

    @Test
    void overrideRedirectsOnlyItsKeyGroup() {
        FugueRoutingOverrides.setOverride(42, 1);
        assertTrue(FugueRoutingOverrides.isActive());
        assertEquals(1, FugueRoutingOverrides.channelFor(42, 3), "overridden key-group");
        assertEquals(7, FugueRoutingOverrides.channelFor(99, 7), "other key-groups keep their default");
        assertEquals(Integer.valueOf(1), FugueRoutingOverrides.getOverride(42));
    }

    @Test
    void clearingTheLastOverrideRestoresTheFastPath() {
        FugueRoutingOverrides.setOverride(42, 1);
        FugueRoutingOverrides.clearOverride(42);
        assertFalse(FugueRoutingOverrides.isActive());
        assertEquals(3, FugueRoutingOverrides.channelFor(42, 3));
    }

    @Test
    void applyFromBarrierInstallsEachPlansTargetSubtask() {
        final MigrationPlan plan =
                new MigrationPlan(
                        42,
                        new OperatorID(),
                        new MigrationPlan.OperatorInstance(0, "tm-a"),
                        new MigrationPlan.OperatorInstance(2, "tm-b"),
                        new JobID(),
                        0L);
        FugueRoutingOverrides.applyFromBarrier(
                MigrationBarrier.createStandalone(1L, Collections.singletonList(plan)));
        assertEquals(Integer.valueOf(2), FugueRoutingOverrides.getOverride(42));
        assertEquals(2, FugueRoutingOverrides.channelFor(42, 0));
    }

    @Test
    void finalBarrierCarriesNoFlip() {
        FugueRoutingOverrides.applyFromBarrier(MigrationBarrier.createFinalBarrier(9L));
        assertFalse(FugueRoutingOverrides.isActive());
        assertNull(FugueRoutingOverrides.getOverride(42));
    }
}
