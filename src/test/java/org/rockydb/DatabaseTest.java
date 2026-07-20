package org.rockydb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {

    @TempDir
    Path tempDir;

    private static byte[] v(String s) {
        return s.getBytes();
    }

    private File newFile() {
        return tempDir.resolve("rocky.db").toFile();
    }

    @Test
    void createInsertGet() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            db.createTable("users");
            db.insert("users", v("alice"), v("1"));
            db.insert("users", v("bob"), v("2"));

            assertArrayEquals(v("1"), db.get("users", v("alice")));
            assertArrayEquals(v("2"), db.get("users", v("bob")));
            assertNull(db.get("users", v("carol")));
        }
    }

    @Test
    void multipleTablesAreIsolated() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            db.createTable("a");
            db.createTable("b");
            db.insert("a", v("shared"), v("from-a"));
            db.insert("b", v("shared"), v("from-b"));

            assertArrayEquals(v("from-a"), db.get("a", v("shared")));
            assertArrayEquals(v("from-b"), db.get("b", v("shared")));
        }
    }

    @Test
    void createTableTwiceThrows() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            db.createTable("t");
            assertThrows(IllegalStateException.class, () -> db.createTable("t"));
        }
    }

    @Test
    void operationsOnMissingTableThrow() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            assertThrows(IllegalStateException.class, () -> db.insert("nope", v("k"), v("v")));
            assertThrows(IllegalStateException.class, () -> db.get("nope", v("k")));
            assertThrows(IllegalStateException.class, () -> db.delete("nope", v("k")));
            assertThrows(IllegalStateException.class, () -> db.count("nope"));
            assertThrows(IllegalStateException.class, () -> db.dropTable("nope"));
        }
    }

    @Test
    void dropTableRemovesItAndAllowsRecreate() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            db.createTable("t");
            db.insert("t", v("k"), v("1"));
            assertEquals(1, db.count("t"));
            db.dropTable("t");

            assertFalse(db.tableExists("t"));
            assertThrows(IllegalStateException.class, () -> db.get("t", v("k")));

            db.createTable("t");
            assertEquals(0, db.count("t"));
            assertNull(db.get("t", v("k")));
        }
    }

    @Test
    void dataPersistsAcrossReopen() throws Exception {
        File file = newFile();
        try (Database db = new Database(file, 32)) {
            db.createTable("t");
            for (int i = 0; i < 50; i++) {
                db.insert("t", v("k" + i), v("val" + i));
            }
        }
        try (Database db = new Database(file, 32)) {
            assertTrue(db.tableExists("t"));
            for (int i = 0; i < 50; i++) {
                assertArrayEquals(v("val" + i), db.get("t", v("k" + i)), "missing key " + i);
            }
            assertThrows(IllegalStateException.class, () -> db.createTable("t"));
        }
    }

    @Test
    void tableRootSplitPersistsAcrossReopen() throws Exception {
        File file = newFile();
        byte[] big = new byte[200];
        long beforeSplit;
        long afterSplit;
        long afterReopen;
        try (Database db = new Database(file, 64)) {
            db.createTable("t");
            beforeSplit = db.rootPageIdOf("t");
            for (int i = 0; i < 120; i++) {
                db.insert("t", v("k" + i), big);
            }
            afterSplit = db.rootPageIdOf("t");
            assertNotEquals(beforeSplit, afterSplit, "root split should have updated the catalog root id");
            for (int i = 0; i < 120; i++) {
                assertArrayEquals(big, db.get("t", v("k" + i)));
            }
        }
        try (Database db = new Database(file, 64)) {
            afterReopen = db.rootPageIdOf("t");
            assertEquals(afterSplit, afterReopen, "root id must persist across reopen");
            for (int i = 0; i < 120; i++) {
                assertArrayEquals(big, db.get("t", v("k" + i)), "missing key " + i + " after reopen");
            }
        }
    }

    @Test
    void concurrentOpsAcrossSharedTablesStayConsistent() throws Exception {
        int numTables = 3;
        int threads = 8;
        int perThread = 300;
        try (Database db = new Database(newFile(), 128)) {
            for (int t = 0; t < numTables; t++) {
                db.createTable("t" + t);
            }

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int tid = t;
                    futures.add(exec.submit(() -> {
                        start.await();
                        String table = "t" + (tid % numTables);
                        for (int i = 0; i < perThread; i++) {
                            db.insert(table, v(table + ":" + tid + ":" + i), v("val"));
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get();
                }
            } finally {
                exec.shutdown();
            }

            for (int t = 0; t < threads; t++) {
                String table = "t" + (t % numTables);
                for (int i = 0; i < perThread; i++) {
                    assertArrayEquals(v("val"), db.get(table, v(table + ":" + t + ":" + i)),
                            "missing key t=" + t + " i=" + i);
                }
            }
        }
    }

    @Test
    void dropTableRecyclesPagesAcrossCreateDropRecreateCycles() throws Exception {
        File file = newFile();
        long firstTreePages;
        try (Database db = new Database(file, 64)) {
            db.createTable("t");
            byte[] big = new byte[200];
            for (int i = 0; i < 120; i++) {
                db.insert("t", v("k" + i), big);
            }
            firstTreePages = db.highWaterMark();
            db.dropTable("t");
        }

        try (Database db = new Database(file, 64)) {
            long hwmAtReopen = db.highWaterMark();
            byte[] big = new byte[200];
            for (int cycle = 0; cycle < 5; cycle++) {
                db.createTable("t");
                for (int i = 0; i < 120; i++) {
                    db.insert("t", v("k" + i), big);
                }
                db.dropTable("t");
            }
            long finalHwm = db.highWaterMark();
            assertTrue(finalHwm <= hwmAtReopen + firstTreePages,
                    "high-water mark grew unboundedly across drop cycles: reopen=" + hwmAtReopen
                            + " final=" + finalHwm + " (first tree=" + firstTreePages + ")");

            db.createTable("t");
            assertFalse(db.tableExists("nope"));
            assertNull(db.get("t", v("k0")));
        }
    }

    @Test
    void dropTableReclaimsDeepMultiLevelTree() throws Exception {
        // payload sized so the tree reaches height >= 3, exercising branch-page reclamation
        File file = newFile();
        long hwmBefore;
        try (Database db = new Database(file, 128)) {
            db.createTable("t");
            int payload = 1500;
            for (int i = 0; i < 120; i++) {
                db.insert("t", bigKey(i, payload), bigValue(i, payload));
            }
            hwmBefore = db.highWaterMark();
            db.dropTable("t");
        }

        try (Database db = new Database(file, 128)) {
            long hwmAtReopen = db.highWaterMark();
            db.createTable("t2");
            int payload = 1500;
            for (int i = 0; i < 120; i++) {
                db.insert("t2", bigKey(i, payload), bigValue(i, payload));
            }
            long hwmAfter = db.highWaterMark();
            assertTrue(hwmAfter <= hwmAtReopen + (hwmBefore / 2) + 16,
                    "deep tree not reclaimed: reopen=" + hwmAtReopen + " after=" + hwmAfter
                            + " before=" + hwmBefore);
        }
    }

    @Test
    void countTracksInsertsReplacesAndDeletes() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            db.createTable("users");
            assertEquals(0, db.count("users"));

            db.insert("users", v("alice"), v("1"));
            db.insert("users", v("bob"), v("2"));
            assertEquals(2, db.count("users"));

            db.insert("users", v("alice"), v("1-updated"));
            assertEquals(2, db.count("users"));
            assertArrayEquals(v("1-updated"), db.get("users", v("alice")));

            db.delete("users", v("bob"));
            assertEquals(1, db.count("users"));

            db.delete("users", v("carol"));
            assertEquals(1, db.count("users"));
        }
    }

    @Test
    void countPersistsAcrossReopen() throws Exception {
        File file = newFile();
        try (Database db = new Database(file, 32)) {
            db.createTable("t");
            for (int i = 0; i < 50; i++) {
                db.insert("t", v("k" + i), v("val" + i));
            }
            db.delete("t", v("k0"));
            db.delete("t", v("k1"));
            assertEquals(48, db.count("t"));
        }
        try (Database db = new Database(file, 32)) {
            assertEquals(48, db.count("t"));
            db.insert("t", v("k0"), v("back"));
            assertEquals(49, db.count("t"));
        }
        try (Database db = new Database(file, 32)) {
            assertEquals(49, db.count("t"));
        }
    }

    @Test
    void countStaysExactThroughSplitsAndMerges() throws Exception {
        File file = newFile();
        byte[] big = new byte[200];
        try (Database db = new Database(file, 64)) {
            db.createTable("t");
            for (int i = 0; i < 120; i++) {
                db.insert("t", v("k" + i), big);
            }
            assertEquals(120, db.count("t"));
            for (int i = 0; i < 120; i++) {
                db.insert("t", v("k" + i), big);
            }
            assertEquals(120, db.count("t"));
            for (int i = 0; i < 100; i++) {
                db.delete("t", v("k" + i));
            }
            assertEquals(20, db.count("t"));
        }
        try (Database db = new Database(file, 64)) {
            assertEquals(20, db.count("t"));
        }
    }

    @Test
    void concurrentInsertsAndDeletesKeepCountExact() throws Exception {
        int threads = 4;
        int perThread = 200;
        int shared = 100;
        try (Database db = new Database(newFile(), 128)) {
            db.createTable("t");

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int tid = t;
                    futures.add(exec.submit(() -> {
                        start.await();
                        for (int i = 0; i < shared; i++) {
                            db.insert("t", v("shared:" + i), v("val" + tid));
                        }
                        for (int i = 0; i < perThread; i++) {
                            db.insert("t", v("own:" + tid + ":" + i), v("val"));
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get();
                }
            } finally {
                exec.shutdown();
            }

            assertEquals(shared + threads * perThread, db.count("t"));

            for (int i = 0; i < shared; i++) {
                db.delete("t", v("shared:" + i));
            }
            assertEquals(threads * perThread, db.count("t"));
        }
    }

    private static byte[] bigKey(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 1;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return b;
    }

    private static byte[] bigValue(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 2;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return b;
    }
}
