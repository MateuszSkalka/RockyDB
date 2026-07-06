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

    private static Value v(String s) {
        return new Value(s.getBytes());
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

            assertEquals(v("1"), db.get("users", v("alice")));
            assertEquals(v("2"), db.get("users", v("bob")));
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

            assertEquals(v("from-a"), db.get("a", v("shared")));
            assertEquals(v("from-b"), db.get("b", v("shared")));
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
            assertThrows(IllegalStateException.class, () -> db.dropTable("nope"));
        }
    }

    @Test
    void dropTableRemovesItAndAllowsRecreate() throws Exception {
        try (Database db = new Database(newFile(), 16)) {
            db.createTable("t");
            db.insert("t", v("k"), v("1"));
            db.dropTable("t");

            assertFalse(db.tableExists("t"));
            assertThrows(IllegalStateException.class, () -> db.get("t", v("k")));

            db.createTable("t"); // name is free again
            assertNull(db.get("t", v("k"))); // fresh empty table
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
                assertEquals(v("val" + i), db.get("t", v("k" + i)), "missing key " + i);
            }
            assertThrows(IllegalStateException.class, () -> db.createTable("t")); // still registered
        }
    }

    @Test
    void tableRootSplitPersistsAcrossReopen() throws Exception {
        File file = newFile();
        Value big = new Value(new byte[200]);
        long beforeSplit;
        long afterSplit;
        long afterReopen;
        try (Database db = new Database(file, 64)) {
            db.createTable("t");
            beforeSplit = db.rootPageIdOf("t");
            // Large values force a leaf split and a root split well within 120 inserts.
            for (int i = 0; i < 120; i++) {
                db.insert("t", v("k" + i), big);
            }
            afterSplit = db.rootPageIdOf("t");
            assertNotEquals(beforeSplit, afterSplit, "root split should have updated the catalog root id");
            for (int i = 0; i < 120; i++) {
                assertEquals(big, db.get("t", v("k" + i)));
            }
        }
        try (Database db = new Database(file, 64)) {
            afterReopen = db.rootPageIdOf("t");
            assertEquals(afterSplit, afterReopen, "root id must persist across reopen");
            for (int i = 0; i < 120; i++) {
                assertEquals(big, db.get("t", v("k" + i)), "missing key " + i + " after reopen");
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
                    assertEquals(v("val"), db.get(table, v(table + ":" + t + ":" + i)),
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
            Value big = new Value(new byte[200]);
            for (int i = 0; i < 120; i++) {
                db.insert("t", v("k" + i), big); // forces splits -> a multi-page tree
            }
            firstTreePages = db.highWaterMark();
            db.dropTable("t");
        }

        // Reopen and run several create/populate/drop cycles. Without page recycling, each cycle's
        // pages would leak and the high-water mark would keep climbing. With reclamation, the dropped
        // table's pages come back through the free-log and the mark stays bounded.
        try (Database db = new Database(file, 64)) {
            long hwmAtReopen = db.highWaterMark();
            Value big = new Value(new byte[200]);
            for (int cycle = 0; cycle < 5; cycle++) {
                db.createTable("t");
                for (int i = 0; i < 120; i++) {
                    db.insert("t", v("k" + i), big);
                }
                db.dropTable("t");
            }
            long finalHwm = db.highWaterMark();
            // Allow modest slack for the catalog + free-log overhead, but the mark must not have grown
            // by a full 5 trees' worth of pages.
            assertTrue(finalHwm <= hwmAtReopen + firstTreePages,
                    "high-water mark grew unboundedly across drop cycles: reopen=" + hwmAtReopen
                            + " final=" + finalHwm + " (first tree=" + firstTreePages + ")");

            // And the table is genuinely gone, recreatable as empty.
            db.createTable("t");
            assertFalse(db.tableExists("nope"));
            assertNull(db.get("t", v("k0")));
        }
    }

    @Test
    void dropTableReclaimsDeepMultiLevelTree() throws Exception {
        // Large keys+values so the tree reaches height >= 3 (root branch -> branch -> leaves),
        // exercising reclamation across internal nodes, not just leaves.
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

        // Reopen: the dropped tree's pages (leaves + internal branches + root) should be recycled.
        // A fresh table populated to similar size should reuse them rather than grow the file much.
        try (Database db = new Database(file, 128)) {
            long hwmAtReopen = db.highWaterMark();
            db.createTable("t2");
            int payload = 1500;
            for (int i = 0; i < 120; i++) {
                db.insert("t2", bigKey(i, payload), bigValue(i, payload));
            }
            long hwmAfter = db.highWaterMark();
            // Reusing the dropped tree's pages, t2 should fit within roughly the same footprint.
            assertTrue(hwmAfter <= hwmAtReopen + (hwmBefore / 2) + 16,
                    "deep tree not reclaimed: reopen=" + hwmAtReopen + " after=" + hwmAfter
                            + " before=" + hwmBefore);
        }
    }

    private static Value bigKey(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 1;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return new Value(b);
    }

    private static Value bigValue(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 2;
        b[len - 1] = (byte) (id & 0xFF);
        return new Value(b);
    }
}
