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
}
