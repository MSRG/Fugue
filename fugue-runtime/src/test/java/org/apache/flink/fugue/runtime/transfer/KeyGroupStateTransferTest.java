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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the genuine per-key-group state-transfer primitive against a real RocksDB instance, using
 * Flink's own key-group key layout. A single operator backend holds many key-groups; migrating a
 * partition moves exactly one key-group out, leaving the others (and the target's own key-groups)
 * untouched — byte-for-byte.
 */
class KeyGroupStateTransferTest {

    private static final int MAX_PARALLELISM = 128; // 1-byte key-group prefix

    @BeforeAll
    static void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    @Test
    void transfersExactlyOneKeyGroupFaithfulToFlinkLayout(@TempDir Path tmp) throws Exception {
        final int prefixBytes = KeyGroupStateTransfer.keyGroupPrefixBytes(MAX_PARALLELISM);
        assertEquals(1, prefixBytes, "max parallelism 128 should use a 1-byte key-group prefix");

        final int[] sourceKeyGroups = {3, 7, 9};
        final int migrating = 7;
        final int entriesPerKeyGroup = 5;

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB source = RocksDB.open(options, tmp.resolve("source").toString());
                RocksDB target = RocksDB.open(options, tmp.resolve("target").toString())) {

            // Source backend hosts several key-groups; remember key-group 7's exact entries.
            final Map<String, byte[]> expectedMigrating = new HashMap<>();
            for (int kg : sourceKeyGroups) {
                for (int i = 0; i < entriesPerKeyGroup; i++) {
                    byte[] key = composeKey(kg, prefixBytes, "key-" + kg + "-" + i);
                    byte[] value = ("value-" + kg + "-" + i).getBytes(StandardCharsets.UTF_8);
                    source.put(key, value);
                    if (kg == migrating) {
                        expectedMigrating.put(hex(key), value);
                    }
                }
            }

            // Target backend already owns its own key-group (2); it must survive the migration.
            byte[] targetOwnKey = composeKey(2, prefixBytes, "target-own");
            byte[] targetOwnValue = "target-own-value".getBytes(StandardCharsets.UTF_8);
            target.put(targetOwnKey, targetOwnValue);

            // --- Extract exactly key-group 7 from the source. ---
            List<KeyGroupStateTransfer.Entry> extracted =
                    KeyGroupStateTransfer.extractKeyGroup(
                            source, source.getDefaultColumnFamily(), migrating, prefixBytes);

            assertEquals(entriesPerKeyGroup, extracted.size(), "should extract only key-group 7");
            for (KeyGroupStateTransfer.Entry e : extracted) {
                assertEquals(migrating, keyGroupOf(e.key, prefixBytes), "extracted key not in key-group 7");
                assertArrayEquals(expectedMigrating.get(hex(e.key)), e.value, "value corrupted in transfer");
            }

            // --- Ingest key-group 7 into the target. ---
            KeyGroupStateTransfer.ingestKeyGroup(target, target.getDefaultColumnFamily(), extracted);

            // Target keeps its own key-group 2...
            assertArrayEquals(targetOwnValue, target.get(targetOwnKey), "target's own key-group was disturbed");
            // ...gains exactly key-group 7's entries, byte-for-byte...
            List<KeyGroupStateTransfer.Entry> targetKg7 =
                    KeyGroupStateTransfer.extractKeyGroup(
                            target, target.getDefaultColumnFamily(), migrating, prefixBytes);
            assertEquals(entriesPerKeyGroup, targetKg7.size());
            for (KeyGroupStateTransfer.Entry e : targetKg7) {
                assertArrayEquals(expectedMigrating.get(hex(e.key)), e.value);
            }
            // ...and does NOT gain the non-migrating key-groups.
            for (int kg : new int[] {3, 9}) {
                assertTrue(
                        KeyGroupStateTransfer.extractKeyGroup(
                                        target, target.getDefaultColumnFamily(), kg, prefixBytes)
                                .isEmpty(),
                        "target unexpectedly received key-group " + kg);
            }

            // Source is left intact (this primitive copies; the source key-group is dropped at commit).
            assertEquals(
                    entriesPerKeyGroup,
                    KeyGroupStateTransfer.extractKeyGroup(
                                    source, source.getDefaultColumnFamily(), migrating, prefixBytes)
                            .size());
        }
    }

    @Test
    void transfersOneKeyGroupViaSstBulkLoad(@TempDir Path tmp) throws Exception {
        final int prefixBytes = KeyGroupStateTransfer.keyGroupPrefixBytes(MAX_PARALLELISM);
        final int[] sourceKeyGroups = {3, 7, 9};
        final int migrating = 7;
        final int entriesPerKeyGroup = 5;

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB source = RocksDB.open(options, tmp.resolve("source").toString());
                RocksDB target = RocksDB.open(options, tmp.resolve("target").toString())) {

            final Map<String, byte[]> expected = new HashMap<>();
            for (int kg : sourceKeyGroups) {
                for (int i = 0; i < entriesPerKeyGroup; i++) {
                    byte[] key = composeKey(kg, prefixBytes, "key-" + kg + "-" + i);
                    byte[] value = ("value-" + kg + "-" + i).getBytes(StandardCharsets.UTF_8);
                    source.put(key, value);
                    if (kg == migrating) {
                        expected.put(hex(key), value);
                    }
                }
            }
            byte[] targetOwnKey = composeKey(2, prefixBytes, "target-own");
            byte[] targetOwnValue = "target-own-value".getBytes(StandardCharsets.UTF_8);
            target.put(targetOwnKey, targetOwnValue);

            Path sst = tmp.resolve("kg-7.sst");
            long n =
                    KeyGroupStateTransfer.exportKeyGroupToSst(
                            source, source.getDefaultColumnFamily(), migrating, prefixBytes, sst);
            assertEquals(entriesPerKeyGroup, n, "should export exactly key-group 7's entries");

            KeyGroupStateTransfer.ingestSst(target, target.getDefaultColumnFamily(), sst);

            assertArrayEquals(targetOwnValue, target.get(targetOwnKey), "target's own key-group disturbed");
            List<KeyGroupStateTransfer.Entry> targetKg7 =
                    KeyGroupStateTransfer.extractKeyGroup(
                            target, target.getDefaultColumnFamily(), migrating, prefixBytes);
            assertEquals(entriesPerKeyGroup, targetKg7.size());
            for (KeyGroupStateTransfer.Entry e : targetKg7) {
                assertArrayEquals(expected.get(hex(e.key)), e.value, "value corrupted via SST bulk load");
            }
            for (int kg : new int[] {3, 9}) {
                assertTrue(
                        KeyGroupStateTransfer.extractKeyGroup(
                                        target, target.getDefaultColumnFamily(), kg, prefixBytes)
                                .isEmpty(),
                        "target unexpectedly received key-group " + kg);
            }
        }
    }

    @Test
    void applyKeyGroupDeltaMakesTargetMatchSource(@TempDir Path tmp) throws Exception {
        final int prefixBytes = KeyGroupStateTransfer.keyGroupPrefixBytes(MAX_PARALLELISM);
        final int kg = 7;
        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB source = RocksDB.open(options, tmp.resolve("source").toString());
                RocksDB target = RocksDB.open(options, tmp.resolve("target").toString())) {
            // Source key-group 7: a,b,c.
            source.put(composeKey(kg, prefixBytes, "a"), "1".getBytes(StandardCharsets.UTF_8));
            source.put(composeKey(kg, prefixBytes, "b"), "2".getBytes(StandardCharsets.UTF_8));
            source.put(composeKey(kg, prefixBytes, "c"), "3".getBytes(StandardCharsets.UTF_8));
            // Target key-group 7 is stale: missing "a", "b" changed, "c" already current, extra "z" (deleted).
            target.put(composeKey(kg, prefixBytes, "b"), "OLD".getBytes(StandardCharsets.UTF_8));
            target.put(composeKey(kg, prefixBytes, "c"), "3".getBytes(StandardCharsets.UTF_8));
            target.put(composeKey(kg, prefixBytes, "z"), "9".getBytes(StandardCharsets.UTF_8));
            // Target's own key-group 2 must survive.
            byte[] ownKey = composeKey(2, prefixBytes, "own");
            target.put(ownKey, "keep".getBytes(StandardCharsets.UTF_8));

            long changed =
                    KeyGroupStateTransfer.applyKeyGroupDelta(
                            source, source.getDefaultColumnFamily(),
                            target, target.getDefaultColumnFamily(), kg, prefixBytes);
            assertEquals(3, changed, "put a, put b(changed), delete z");

            List<KeyGroupStateTransfer.Entry> s =
                    KeyGroupStateTransfer.extractKeyGroup(
                            source, source.getDefaultColumnFamily(), kg, prefixBytes);
            List<KeyGroupStateTransfer.Entry> t =
                    KeyGroupStateTransfer.extractKeyGroup(
                            target, target.getDefaultColumnFamily(), kg, prefixBytes);
            Map<String, String> sm = new HashMap<>();
            Map<String, String> tm = new HashMap<>();
            for (KeyGroupStateTransfer.Entry e : s) sm.put(hex(e.key), hex(e.value));
            for (KeyGroupStateTransfer.Entry e : t) tm.put(hex(e.key), hex(e.value));
            assertEquals(sm, tm, "target key-group 7 should byte-equal source after delta");
            assertArrayEquals(
                    "keep".getBytes(StandardCharsets.UTF_8), target.get(ownKey), "own key-group disturbed");

            // Idempotent: a second pass changes nothing.
            assertEquals(
                    0,
                    KeyGroupStateTransfer.applyKeyGroupDelta(
                            source, source.getDefaultColumnFamily(),
                            target, target.getDefaultColumnFamily(), kg, prefixBytes));
        }
    }

    /** Flink key layout (simplified): key-group prefix followed by the serialized key bytes. */
    private static byte[] composeKey(int keyGroup, int prefixBytes, String key) {
        byte[] prefix = KeyGroupStateTransfer.keyGroupPrefix(keyGroup, prefixBytes);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] composed = new byte[prefix.length + keyBytes.length];
        System.arraycopy(prefix, 0, composed, 0, prefix.length);
        System.arraycopy(keyBytes, 0, composed, prefix.length, keyBytes.length);
        return composed;
    }

    private static int keyGroupOf(byte[] key, int prefixBytes) {
        int keyGroup = 0;
        for (int i = 0; i < prefixBytes; i++) {
            keyGroup = (keyGroup << 8) | (key[i] & 0xFF);
        }
        return keyGroup;
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
