package org.rockydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class BufferedPoolTest {

    private File dbFile;
    private BufferedPool pool;

    private static Value v(String s) {
        return new Value(s.getBytes());
    }

    @BeforeEach
    void setUp() throws IOException {
        dbFile = File.createTempFile("rockydb-pool-", ".db");
        dbFile.deleteOnExit();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (pool != null) {
            pool.close();
            pool = null;
        }
        if (dbFile != null && dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    void readNodeReturnsInitializedRootLeaf() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        long rootId = pool.rootId();

        Node root = pool.readNode(rootId);

        assertTrue(root.isLeaf());
        assertEquals(rootId, root.id());
    }

    @Test
    void writeNodeThenReadNodeRoundTripsWithinCache() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        long rootId = pool.rootId();
        Value[] keys = {v("a"), v("c")};
        Value[] values = {v("1"), v("3")};
        LeafNode written = new LeafNode(rootId, 1, keys, values, -1L);

        pool.writeNode(written);

        LeafNode read = (LeafNode) pool.readNode(rootId);
        assertArrayEquals(keys, read.getKeys());
        assertArrayEquals(values, read.getValues());
    }

    @Test
    void repeatedReadsGenerateBufferHits() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        long rootId = pool.rootId();

        pool.readNode(rootId);
        pool.readNode(rootId);

        assertTrue(pool.getBufferHits() >= 1);
        assertTrue(pool.getBufferMisses() >= 1);
        double ratio = pool.getHitRatio();
        assertTrue(ratio > 0.0 && ratio <= 1.0);
    }

    @Test
    void tracksUsedAndDirtyFrames() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        long rootId = pool.rootId();

        assertEquals(16, pool.getNumFrames());
        assertEquals(0, pool.getUsedFrames());

        pool.readNode(rootId);
        assertTrue(pool.getUsedFrames() >= 1);
        assertEquals(0, pool.getDirtyFrames());

        pool.writeNode(new LeafNode(rootId, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L));
        assertTrue(pool.getDirtyFrames() >= 1);
    }

    @Test
    void latchForWriteGetSetAndCloseRoundTrip() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        long rootId = pool.rootId();
        Value[] keys = {v("a")};
        Value[] values = {v("1")};

        try (WriteHandle handle = pool.latchForWrite(rootId)) {
            Node before = handle.get();
            assertTrue(before.isLeaf());

            handle.set(new LeafNode(rootId, 1, keys, values, -1L));

            LeafNode after = (LeafNode) handle.get();
            assertArrayEquals(keys, after.getKeys());
        }

        LeafNode read = (LeafNode) pool.readNode(rootId);
        assertArrayEquals(keys, read.getKeys());
    }

    @Test
    void handleCloseIsIdempotentAndReleasesLatch() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        long rootId = pool.rootId();

        WriteHandle handle = pool.latchForWrite(rootId);
        handle.close();
        handle.close();

        pool.latchForWrite(rootId).close();
    }

    @Test
    void evictsPagesAndReloadsThemCorrectly() throws Exception {
        pool = new BufferedPool(dbFile, 8);
        BLinkTree tree = new BLinkTree(pool);
        byte[] bigValue = new byte[4000];
        List<Value> keys = new ArrayList<>();
        int count = 150;

        for (int i = 0; i < count; i++) {
            Value key = v("key" + i);
            keys.add(key);
            tree.addValue(key, new Value(bigValue));
        }

        assertTrue(pool.getEvictions() > 0);
        for (int i = 0; i < count; i++) {
            assertEquals(new Value(bigValue), tree.get(keys.get(i)), "wrong value for key " + i);
        }
    }

    @Test
    void closeFlushesDirtyPagesToDisk() throws IOException {
        pool = new BufferedPool(dbFile, 16);
        BLinkTree tree = new BLinkTree(pool);
        tree.addValue(v("persistent"), v("yes"));
        pool.close();
        pool = null;

        BufferedPool reopened = new BufferedPool(dbFile, 16);
        try {
            BLinkTree reopenedTree = new BLinkTree(reopened);
            assertEquals(v("yes"), reopenedTree.get(v("persistent")));
        } finally {
            reopened.close();
        }
    }

    @Test
    void throwsBufferExhaustedWhenPoolSaturatedByPins() throws Exception {
        pool = new BufferedPool(dbFile, 1);
        long rootId = pool.rootId();

        WriteHandle held = pool.latchForWrite(rootId);

        assertThrows(BufferExhaustedException.class, () -> pool.readNode(rootId + 1));

        held.close();
    }

    @Test
    void deleteRemovesKey() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        BLinkTree tree = new BLinkTree(pool);
        tree.addValue(v("a"), v("1"));
        tree.addValue(v("b"), v("2"));

        tree.delete(v("a"));

        assertNull(tree.get(v("a")));
        assertEquals(v("2"), tree.get(v("b")));
    }

    @Test
    void deleteAbsentKeyIsNoop() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        BLinkTree tree = new BLinkTree(pool);
        tree.addValue(v("a"), v("1"));

        tree.delete(v("missing"));

        assertEquals(v("1"), tree.get(v("a")));
        assertNull(tree.get(v("missing")));
    }

    @Test
    void deleteEmptiedLeafSelfHealsOnReinsert() throws Exception {
        pool = new BufferedPool(dbFile, 16);
        BLinkTree tree = new BLinkTree(pool);
        tree.addValue(v("a"), v("1"));

        tree.delete(v("a"));
        assertNull(tree.get(v("a")));

        tree.addValue(v("a"), v("2"));

        assertEquals(v("2"), tree.get(v("a")));
    }

    @Test
    void deleteAcrossSplitsLeavesRemainingKeysCorrect() throws Exception {
        pool = new BufferedPool(dbFile, 64);
        BLinkTree tree = new BLinkTree(pool);
        Value bigValue = new Value(new byte[200]);
        int count = 400;

        for (int i = 0; i < count; i++) {
            tree.addValue(v("key" + i), bigValue);
        }

        for (int i = 0; i < count; i += 2) {
            tree.delete(v("key" + i));
        }

        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                assertNull(tree.get(v("key" + i)), "deleted key " + i + " still present");
            } else {
                assertEquals(bigValue, tree.get(v("key" + i)), "wrong value for key " + i);
            }
        }
    }

    @Test
    void concurrentInsertsAndDeletesStayConsistent() throws Exception {
        pool = new BufferedPool(dbFile, 128);
        BLinkTree tree = new BLinkTree(pool);
        Value bigValue = new Value(new byte[200]);
        int keyCount = 500;
        int threads = 8;

        // Seed the even keys so deletes have a target even under contention.
        for (int i = 0; i < keyCount; i += 2) {
            tree.addValue(v("key" + i), bigValue);
        }

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int offset = t;
                futures.add(exec.submit(() -> {
                    start.await();
                    // Each key is owned by exactly one thread (i % threads == offset), so the
                    // final state is deterministic: even keys end up deleted, odd keys inserted.
                    for (int i = offset; i < keyCount; i += threads) {
                        tree.addValue(v("key" + i), bigValue);
                        if (i % 2 == 0) {
                            tree.delete(v("key" + i));
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();   // surface any worker exception
            }
        } finally {
            exec.shutdown();
        }

        for (int i = 0; i < keyCount; i++) {
            if (i % 2 == 0) {
                assertNull(tree.get(v("key" + i)), "deleted key " + i + " still present");
            } else {
                assertEquals(bigValue, tree.get(v("key" + i)), "wrong value for key " + i);
            }
        }
    }
}
