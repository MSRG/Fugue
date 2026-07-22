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

package org.apache.flink.fugue.common;

import org.apache.flink.fugue.common.state.MigrationState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MigrationState.
 */
public class MigrationStateTest {

    @Test
    public void testInitialState() {
        MigrationState state = MigrationState.IDLE;
        // IDLE is the resting state: not terminal (it transitions to INIT) and not an active migration.
        assertFalse(state.isTerminal());
        assertFalse(state.isActiveMigration());
    }

    @Test
    public void testValidTransitions() {
        // IDLE -> INIT
        assertTrue(MigrationState.IDLE.canTransitionTo(MigrationState.INIT));

        // INIT -> PRE_COPY
        assertTrue(MigrationState.INIT.canTransitionTo(MigrationState.PRE_COPY));

        // PRE_COPY -> AWAIT_BARRIER
        assertTrue(MigrationState.PRE_COPY.canTransitionTo(MigrationState.AWAIT_BARRIER));

        // AWAIT_BARRIER -> FINALIZING
        assertTrue(MigrationState.AWAIT_BARRIER.canTransitionTo(MigrationState.FINALIZING));

        // FINALIZING -> COMMITTED
        assertTrue(MigrationState.FINALIZING.canTransitionTo(MigrationState.COMMITTED));
    }

    @Test
    public void testAbortFromAnyState() {
        // Any non-terminal state can transition to ABORTED
        assertTrue(MigrationState.INIT.canTransitionTo(MigrationState.ABORTED));
        assertTrue(MigrationState.PRE_COPY.canTransitionTo(MigrationState.ABORTED));
        assertTrue(MigrationState.AWAIT_BARRIER.canTransitionTo(MigrationState.ABORTED));
        assertTrue(MigrationState.FINALIZING.canTransitionTo(MigrationState.ABORTED));
    }

    @Test
    public void testInvalidTransitions() {
        // IDLE cannot go directly to PRE_COPY
        assertFalse(MigrationState.IDLE.canTransitionTo(MigrationState.PRE_COPY));

        // INIT cannot go directly to AWAIT_BARRIER
        assertFalse(MigrationState.INIT.canTransitionTo(MigrationState.AWAIT_BARRIER));

        // PRE_COPY cannot go directly to FINALIZING
        assertFalse(MigrationState.PRE_COPY.canTransitionTo(MigrationState.FINALIZING));

        // Cannot transition to IDLE from non-idle states
        assertFalse(MigrationState.INIT.canTransitionTo(MigrationState.IDLE));
        assertFalse(MigrationState.PRE_COPY.canTransitionTo(MigrationState.IDLE));
    }

    @Test
    public void testTerminalStates() {
        // Terminal states cannot transition. IDLE is NOT terminal (IDLE -> INIT is valid).
        assertFalse(MigrationState.IDLE.isTerminal());
        assertTrue(MigrationState.COMMITTED.isTerminal());
        assertTrue(MigrationState.ABORTED.isTerminal());

        // Terminal states cannot transition to anything
        assertFalse(MigrationState.COMMITTED.canTransitionTo(MigrationState.IDLE));
        assertFalse(MigrationState.ABORTED.canTransitionTo(MigrationState.IDLE));
    }

    @Test
    public void testActiveMigrationStates() {
        // Non-terminal non-idle states are active migrations
        assertTrue(MigrationState.INIT.isActiveMigration());
        assertTrue(MigrationState.PRE_COPY.isActiveMigration());
        assertTrue(MigrationState.AWAIT_BARRIER.isActiveMigration());
        assertTrue(MigrationState.FINALIZING.isActiveMigration());

        // Terminal states are not active
        assertFalse(MigrationState.IDLE.isActiveMigration());
        assertFalse(MigrationState.COMMITTED.isActiveMigration());
        assertFalse(MigrationState.ABORTED.isActiveMigration());
    }

    @Test
    public void testBackgroundOperations() {
        // Only PRE_COPY and AWAIT_BARRIER allow background operations
        assertFalse(MigrationState.IDLE.allowsBackgroundOperations());
        assertFalse(MigrationState.INIT.allowsBackgroundOperations());
        assertTrue(MigrationState.PRE_COPY.allowsBackgroundOperations());
        assertTrue(MigrationState.AWAIT_BARRIER.allowsBackgroundOperations());
        assertFalse(MigrationState.FINALIZING.allowsBackgroundOperations());
        assertFalse(MigrationState.COMMITTED.allowsBackgroundOperations());
        assertFalse(MigrationState.ABORTED.allowsBackgroundOperations());
    }

    @Test
    public void testDescriptions() {
        // All states should have descriptions
        for (MigrationState state : MigrationState.values()) {
            assertNotNull(state.getDescription());
            assertFalse(state.getDescription().isEmpty());
        }
    }

    @Test
    public void testFullSuccessfulPath() {
        // Test complete successful migration path
        MigrationState current = MigrationState.IDLE;

        // IDLE -> INIT
        assertTrue(current.canTransitionTo(MigrationState.INIT));
        current = MigrationState.INIT;

        // INIT -> PRE_COPY
        assertTrue(current.canTransitionTo(MigrationState.PRE_COPY));
        current = MigrationState.PRE_COPY;

        // PRE_COPY -> AWAIT_BARRIER
        assertTrue(current.canTransitionTo(MigrationState.AWAIT_BARRIER));
        current = MigrationState.AWAIT_BARRIER;

        // AWAIT_BARRIER -> FINALIZING
        assertTrue(current.canTransitionTo(MigrationState.FINALIZING));
        current = MigrationState.FINALIZING;

        // FINALIZING -> COMMITTED
        assertTrue(current.canTransitionTo(MigrationState.COMMITTED));
        current = MigrationState.COMMITTED;

        // COMMITTED is terminal
        assertTrue(current.isTerminal());
    }
}