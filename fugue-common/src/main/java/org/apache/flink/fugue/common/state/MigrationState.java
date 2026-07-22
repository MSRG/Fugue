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

package org.apache.flink.fugue.common.state;

/**
 * The 7-state state machine for migration coordination.
 * Transitions: IDLE → INIT → PRE_COPY → AWAIT_BARRIER → FINALIZING → {COMMITTED, ABORTED}
 */
public enum MigrationState {
    /**
     * No migration in progress. The initial (resting) state; transitions to {@link #INIT} when a
     * migration is triggered. Not terminal — a terminal state is one with no outgoing transitions.
     */
    IDLE("Idle", false),

    /**
     * Migration has been initiated, preparing source and target operators.
     */
    INIT("Initializing", false),

    /**
     * Pre-emptive background copy phase in progress.
     * Iteratively transferring state snapshots and delta logs.
     */
    PRE_COPY("Pre-copying state", false),

    /**
     * Waiting for migration barrier alignment.
     * Background copy has converged, waiting for atomic cutover.
     */
    AWAIT_BARRIER("Awaiting barrier", false),

    /**
     * Finalizing the migration after barrier alignment.
     * Transferring final delta and updating routing tables.
     */
    FINALIZING("Finalizing migration", false),

    /**
     * Migration completed successfully.
     * State has been transferred and routing updated.
     */
    COMMITTED("Committed", true),

    /**
     * Migration aborted due to failure or timeout.
     * System rolled back to previous state.
     */
    ABORTED("Aborted", true);

    private final String description;
    private final boolean isTerminal;

    MigrationState(String description, boolean isTerminal) {
        this.description = description;
        this.isTerminal = isTerminal;
    }

    public String getDescription() {
        return description;
    }

    public boolean isTerminal() {
        return isTerminal;
    }

    /**
     * Check if a transition from this state to the target state is valid.
     */
    public boolean canTransitionTo(MigrationState targetState) {
        if (this.isTerminal) {
            return false; // Terminal states cannot transition
        }

        switch (this) {
            case IDLE:
                return targetState == INIT;
            case INIT:
                return targetState == PRE_COPY || targetState == ABORTED;
            case PRE_COPY:
                return targetState == AWAIT_BARRIER || targetState == ABORTED;
            case AWAIT_BARRIER:
                return targetState == FINALIZING || targetState == ABORTED;
            case FINALIZING:
                return targetState == COMMITTED || targetState == ABORTED;
            default:
                return false;
        }
    }

    /**
     * Check if this state indicates an active migration.
     */
    public boolean isActiveMigration() {
        return this != IDLE && !isTerminal;
    }

    /**
     * Check if this state allows background operations.
     */
    public boolean allowsBackgroundOperations() {
        return this == PRE_COPY || this == AWAIT_BARRIER;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", name(), description);
    }
}