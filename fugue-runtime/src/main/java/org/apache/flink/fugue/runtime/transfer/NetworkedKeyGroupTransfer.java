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

package org.apache.flink.fugue.runtime.transfer;

import org.apache.flink.contrib.streaming.state.RocksDBBackendAccessor;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Cross-TaskManager per-key-group state transfer over a dedicated TCP connection — the State
 * Transfer Service. In a multi-node cluster the source and target operator instances live in different
 * JVMs, so the in-JVM backend→backend copy of {@link OperatorStateMigrationService} cannot be used; this
 * extracts the migrating key-group from the <em>local</em> source backend, streams it over a socket, and
 * ingests it into the <em>local</em> target backend on the other side. Both ends reach their local
 * {@link RocksDBKeyedStateBackend} through the same-package {@link RocksDBBackendAccessor} (no reflection).
 *
 * <p>The receiver deletes the key-group before ingesting, so each transfer makes the target's key-group
 * exactly equal the source's at send time (idempotent full-snapshot semantics — correct for both the bulk
 * pre-copy and the final delta; the byte-minimal iterative delta is an optimization on top).
 *
 * <p>Wire format (length-prefixed, no Java serialization of state): {@code int keyGroup; boolean
 * finalRound; int stateCount;} then per state {@code utf stateName; int entryCount;} then per entry
 * {@code int keyLen; key[]; int valueLen; value[]}. The {@code finalRound} flag lets the receiver know
 * the last round has arrived (so {@code O_new} can release its buffered post-barrier records).
 */
public final class NetworkedKeyGroupTransfer {

    private NetworkedKeyGroupTransfer() {}

    /** Open a listening socket on {@code port} (0 = any free port) bound to all interfaces. */
    public static ServerSocket listen(int port) throws Exception {
        final ServerSocket server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(port));
        return server;
    }

    /**
     * Extract {@code keyGroup} from the local source backend and stream it to {@code host:port}.
     * Returns the number of state bytes sent.
     */
    public static long send(
            RocksDBKeyedStateBackend<?> sourceBackend,
            List<String> stateNames,
            int keyGroup,
            String host,
            int port,
            boolean finalRound)
            throws Exception {
        final RocksDB db = RocksDBBackendAccessor.db(sourceBackend);
        final int prefixBytes = RocksDBBackendAccessor.keyGroupPrefixBytes(sourceBackend);
        try (Socket socket = new Socket(host, port);
                DataOutputStream out =
                        new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            out.writeInt(keyGroup);
            out.writeBoolean(finalRound);
            out.writeInt(stateNames.size());
            long bytes = 0;
            for (String stateName : stateNames) {
                final ColumnFamilyHandle cf =
                        RocksDBBackendAccessor.columnFamily(sourceBackend, stateName);
                final List<KeyGroupStateTransfer.Entry> entries =
                        KeyGroupStateTransfer.extractKeyGroup(db, cf, keyGroup, prefixBytes);
                out.writeUTF(stateName);
                out.writeInt(entries.size());
                for (KeyGroupStateTransfer.Entry e : entries) {
                    out.writeInt(e.key.length);
                    out.write(e.key);
                    out.writeInt(e.value.length);
                    out.write(e.value);
                    bytes += e.key.length + e.value.length;
                }
            }
            out.flush();
            return bytes;
        }
    }

    /**
     * Accept one connection on {@code server}, read a streamed key-group, and ingest it into the local
     * target backend (replacing any existing entries for that key-group). Returns whether this was the
     * final round (so the caller can release buffered post-barrier records).
     */
    public static boolean receiveAndIngest(
            ServerSocket server, RocksDBKeyedStateBackend<?> targetBackend) throws Exception {
        final RocksDB db = RocksDBBackendAccessor.db(targetBackend);
        final int prefixBytes = RocksDBBackendAccessor.keyGroupPrefixBytes(targetBackend);
        try (Socket socket = server.accept();
                DataInputStream in =
                        new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
            final int keyGroup = in.readInt();
            final boolean finalRound = in.readBoolean();
            final int stateCount = in.readInt();
            for (int s = 0; s < stateCount; s++) {
                final String stateName = in.readUTF();
                final int entryCount = in.readInt();
                final List<KeyGroupStateTransfer.Entry> entries = new ArrayList<>(entryCount);
                for (int i = 0; i < entryCount; i++) {
                    final byte[] key = new byte[in.readInt()];
                    in.readFully(key);
                    final byte[] value = new byte[in.readInt()];
                    in.readFully(value);
                    entries.add(new KeyGroupStateTransfer.Entry(key, value));
                }
                final ColumnFamilyHandle cf =
                        RocksDBBackendAccessor.columnFamily(targetBackend, stateName);
                // Replace the key-group: delete any existing entries (e.g. a stale bulk round), then
                // ingest the streamed snapshot, so the target's key-group equals the source's exactly.
                KeyGroupStateTransfer.deleteKeyGroup(db, cf, keyGroup, prefixBytes);
                KeyGroupStateTransfer.ingestKeyGroup(db, cf, entries);
            }
            return finalRound;
        }
    }
}
