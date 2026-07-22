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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.KeyGroupRange;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/**
 * Same-package bridge exposing the parts of a live {@link RocksDBKeyedStateBackend} that Fugue's
 * per-key-group transfer needs. It is deliberately placed in Flink's
 * {@code org.apache.flink.contrib.streaming.state} package so it can read the backend's
 * {@code protected} {@code db} field and call its package-private {@code getColumnFamilyHandle(...)}
 * with no reflection and no Flink-core patch. This is the single point of contact with the
 * RocksDB-backend internals; everything else in Fugue works against {@link RocksDB} /
 * {@link ColumnFamilyHandle}.
 *
 * <p>Each operator instance's Migration Controller interacts with its
 * local State Backend; this accessor is how Fugue reaches that live backend.
 */
public final class RocksDBBackendAccessor {

    private RocksDBBackendAccessor() {}

    /** The live RocksDB instance behind the keyed backend (the backend's {@code db} is protected). */
    public static RocksDB db(RocksDBKeyedStateBackend<?> backend) {
        return backend.db;
    }

    /**
     * The column family that stores the named keyed state (the backend's accessor is package-private).
     *
     * @throws IllegalStateException if the state is not registered on this backend (e.g. the subtask
     *     never called {@code getState(...)} for it), so callers get a distinctive failure rather than
     *     a downstream NPE.
     */
    public static ColumnFamilyHandle columnFamily(RocksDBKeyedStateBackend<?> backend, String stateName) {
        final ColumnFamilyHandle handle = backend.getColumnFamilyHandle(stateName);
        if (handle == null) {
            throw new IllegalStateException(
                    "No RocksDB column family for keyed state '"
                            + stateName
                            + "' on this backend; is the state registered (getState called) on this subtask?");
        }
        return handle;
    }

    /** Number of big-endian key-group prefix bytes Flink prepends to every key (public getter). */
    public static int keyGroupPrefixBytes(RocksDBKeyedStateBackend<?> backend) {
        return backend.getKeyGroupPrefixBytes();
    }

    /** The contiguous key-group range this subtask's backend statically owns. */
    public static KeyGroupRange keyGroupRange(RocksDBKeyedStateBackend<?> backend) {
        return backend.getKeyGroupRange();
    }
}
