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

package org.apache.flink.fugue.coordinator.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for the multi-TaskManager controller-address derivation. */
class FugueAddressResolverTest {

    @Test
    void swapsEndpointPathToController() {
        assertEquals(
                "pekko.tcp://flink@host-a:6122/user/rpc/fugue-controller",
                FugueAddressResolver.controllerAddressFrom(
                        "pekko.tcp://flink@host-a:6122/user/rpc/taskmanager_0"));
    }

    @Test
    void worksForDifferentHostsAndPorts() {
        assertEquals(
                "pekko.tcp://flink@10.0.0.7:43219/user/rpc/fugue-controller",
                FugueAddressResolver.controllerAddressFrom(
                        "pekko.tcp://flink@10.0.0.7:43219/user/rpc/taskmanager_5_abc123"));
    }

    @Test
    void returnsInputUnchangedWhenNoPathSegment() {
        assertEquals("garbage-no-slash", FugueAddressResolver.controllerAddressFrom("garbage-no-slash"));
        assertEquals(null, FugueAddressResolver.controllerAddressFrom(null));
    }
}
