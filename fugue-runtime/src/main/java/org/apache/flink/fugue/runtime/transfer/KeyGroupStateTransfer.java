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

import org.apache.flink.runtime.state.CompositeKeySerializationUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per-key-group state transfer, faithful to Flink's RocksDB key layout.
 *
 * <p>A Flink keyed RocksDB backend prefixes every stored key with its key-group, written big-endian
 * (see {@link CompositeKeySerializationUtils#writeKeyGroup}). Because RocksDB stores keys in sorted
 * (unsigned-lexicographic) order, all entries for a single key-group form a contiguous range that
 * shares the key-group prefix. Fugue migrates one partition = one key-group, so it extracts exactly
 * that prefix range from the source backend and ingests it into the target backend (which keeps its
 * own key-groups and gains this one).
 *
 * <p>This is the per-key-group movement primitive; it operates on the same key encoding Flink's own
 * rescaling uses ({@code RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange}).
 */
public final class KeyGroupStateTransfer {

    private KeyGroupStateTransfer() {}

    /** Number of key-group prefix bytes Flink uses for the given max parallelism (1 for &le;128). */
    public static int keyGroupPrefixBytes(int maxParallelism) {
        return CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(maxParallelism);
    }

    /** The big-endian prefix bytes that every key in {@code keyGroup} starts with (Flink layout). */
    public static byte[] keyGroupPrefix(int keyGroup, int keyGroupPrefixBytes) {
        final byte[] prefix = new byte[keyGroupPrefixBytes];
        CompositeKeySerializationUtils.serializeKeyGroup(keyGroup, prefix);
        return prefix;
    }

    /** A raw key/value pair from a state column family. */
    public static final class Entry {
        public final byte[] key;
        public final byte[] value;

        public Entry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Extract every entry belonging to {@code keyGroup} from a column family, by seeking to the
     * key-group prefix and reading until the prefix no longer matches (the entries are contiguous).
     */
    public static List<Entry> extractKeyGroup(
            RocksDB db, ColumnFamilyHandle columnFamily, int keyGroup, int keyGroupPrefixBytes) {
        final byte[] prefix = keyGroupPrefix(keyGroup, keyGroupPrefixBytes);
        final List<Entry> entries = new ArrayList<>();
        try (RocksIterator it = db.newIterator(columnFamily)) {
            for (it.seek(prefix); it.isValid(); it.next()) {
                final byte[] key = it.key();
                if (!hasPrefix(key, prefix)) {
                    break; // sorted order guarantees we have left this key-group's range
                }
                entries.add(new Entry(key, it.value()));
            }
        }
        return entries;
    }

    /**
     * Ingest extracted entries into a target column family. The entries already carry their
     * key-group prefix, so they land in the correct position in the target backend.
     */
    public static void ingestKeyGroup(RocksDB db, ColumnFamilyHandle columnFamily, List<Entry> entries)
            throws RocksDBException {
        try (WriteBatch batch = new WriteBatch();
                WriteOptions options = new WriteOptions()) {
            for (Entry entry : entries) {
                batch.put(columnFamily, entry.key, entry.value);
            }
            db.write(options, batch);
        }
    }

    /**
     * Export exactly {@code keyGroup}'s entries from a column family into a standalone SST file, in
     * RocksDB's file-based bulk format. This is the SST-based bulk transfer (the
     * initial background copy). Entries are added in the iterator's sorted order, which
     * {@link SstFileWriter} requires. Returns the number of entries written; writes no file and
     * returns 0 for an empty key-group ({@code SstFileWriter} cannot finish an empty file).
     */
    public static long exportKeyGroupToSst(
            RocksDB db, ColumnFamilyHandle columnFamily, int keyGroup, int keyGroupPrefixBytes, Path sstFile)
            throws RocksDBException {
        final byte[] prefix = keyGroupPrefix(keyGroup, keyGroupPrefixBytes);
        long count = 0;
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            boolean opened = false;
            try (RocksIterator it = db.newIterator(columnFamily)) {
                for (it.seek(prefix); it.isValid(); it.next()) {
                    final byte[] key = it.key();
                    if (!hasPrefix(key, prefix)) {
                        break;
                    }
                    if (!opened) {
                        writer.open(sstFile.toString());
                        opened = true;
                    }
                    writer.put(key, it.value());
                    count++;
                }
            }
            if (opened) {
                writer.finish();
            }
        }
        return count;
    }

    /**
     * Bulk-load an SST file (from {@link #exportKeyGroupToSst}) into a target column family via
     * RocksDB's IngestExternalFile API — ingestion that bypasses memtable/WAL.
     */
    public static void ingestSst(RocksDB db, ColumnFamilyHandle columnFamily, Path sstFile)
            throws RocksDBException {
        try (IngestExternalFileOptions options =
                new IngestExternalFileOptions()
                        .setMoveFiles(false)
                        .setSnapshotConsistency(true)
                        .setAllowGlobalSeqNo(true)
                        .setAllowBlockingFlush(true)) {
            db.ingestExternalFile(
                    columnFamily, Collections.singletonList(sstFile.toString()), options);
        }
    }

    /**
     * Make the target's {@code keyGroup} equal the source's via a snapshot diff — the
     * iterative delta, transferred as key/value pairs through the normal write path. Puts new/changed
     * keys, deletes keys removed from the source; other key-groups are untouched. Returns the number
     * of changed entries (0 once converged). Intermediate rounds taken under live writes are
     * approximate; a round taken after writes quiesce makes the target exactly equal to the source.
     */
    public static long applyKeyGroupDelta(
            RocksDB sourceDb, ColumnFamilyHandle sourceCf,
            RocksDB targetDb, ColumnFamilyHandle targetCf,
            int keyGroup, int keyGroupPrefixBytes)
            throws RocksDBException {
        final List<Entry> src = extractKeyGroup(sourceDb, sourceCf, keyGroup, keyGroupPrefixBytes);
        final List<Entry> tgt = extractKeyGroup(targetDb, targetCf, keyGroup, keyGroupPrefixBytes);
        final Map<ByteBuffer, byte[]> tgtMap = new HashMap<>();
        for (Entry e : tgt) {
            tgtMap.put(ByteBuffer.wrap(e.key), e.value);
        }
        final Set<ByteBuffer> srcKeys = new HashSet<>();
        long changed = 0;
        try (WriteBatch batch = new WriteBatch();
                WriteOptions options = new WriteOptions()) {
            for (Entry e : src) {
                srcKeys.add(ByteBuffer.wrap(e.key));
                final byte[] tv = tgtMap.get(ByteBuffer.wrap(e.key));
                if (tv == null || !Arrays.equals(tv, e.value)) {
                    batch.put(targetCf, e.key, e.value);
                    changed++;
                }
            }
            for (Entry e : tgt) {
                if (!srcKeys.contains(ByteBuffer.wrap(e.key))) {
                    batch.delete(targetCf, e.key);
                    changed++;
                }
            }
            targetDb.write(options, batch);
        }
        return changed;
    }

    /**
     * Delete every entry belonging to {@code keyGroup} from a column family — the target-side rollback
     * when a migration is aborted ({@code O_new} deletes any partially received state).
     * Returns the number of entries deleted.
     */
    public static long deleteKeyGroup(
            RocksDB db, ColumnFamilyHandle columnFamily, int keyGroup, int keyGroupPrefixBytes)
            throws RocksDBException {
        final byte[] prefix = keyGroupPrefix(keyGroup, keyGroupPrefixBytes);
        final List<byte[]> toDelete = new ArrayList<>();
        try (RocksIterator it = db.newIterator(columnFamily)) {
            for (it.seek(prefix); it.isValid(); it.next()) {
                final byte[] key = it.key();
                if (!hasPrefix(key, prefix)) {
                    break;
                }
                toDelete.add(key);
            }
        }
        try (WriteBatch batch = new WriteBatch();
                WriteOptions options = new WriteOptions()) {
            for (byte[] key : toDelete) {
                batch.delete(columnFamily, key);
            }
            db.write(options, batch);
        }
        return toDelete.size();
    }

    private static boolean hasPrefix(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
