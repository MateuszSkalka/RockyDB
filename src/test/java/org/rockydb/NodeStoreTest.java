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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NodeStoreTest {

    private File dbFile;
    private NodeStore store;

    private static byte[] v(String s) {
        return s.getBytes();
    }

    @BeforeEach
    void setUp() throws IOException {
        dbFile = File.createTempFile("rockydb-nodestore-", ".db");
        dbFile.deleteOnExit();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (dbFile != null && dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    void readPageReturnsInitializedRootLeaf() throws Exception {
        store = new NodeStore(dbFile, 16);
        long rootId = store.rootId();

        try (PageRef ref = store.readPage(rootId)) {
            assertTrue(Pages.isLeaf(ref.bytes()));
            assertEquals(rootId, ref.id());
        }
    }

    @Test
    void latchMutationsThenReadRoundTripsWithinCache() throws Exception {
        store = new NodeStore(dbFile, 16);
        long rootId = store.rootId();

        try (PageWrite w = store.latchForWrite(rootId)) {
            LeafPage.put(w.bytes(), v("a"), v("1"));
            LeafPage.put(w.bytes(), v("c"), v("3"));
            w.markDirty();
        }

        try (PageRef ref = store.readPage(rootId)) {
            assertArrayEquals(v("1"), LeafPage.getValue(ref.bytes(), v("a")));
            assertArrayEquals(v("3"), LeafPage.getValue(ref.bytes(), v("c")));
        }
    }

    @Test
    void latchForWriteBlankSkipsContentButPublishesFormattedPage() throws Exception {
        store = new NodeStore(dbFile, 16);
        long blankId = store.allocate();

        try (PageWrite w = store.latchForWriteBlank(blankId)) {
            LeafPage.formatEmpty(w.bytes(), 1, Pages.NO_LINK, null);
            LeafPage.put(w.bytes(), v("fresh"), v("page"));
            w.markDirty();
        }

        try (PageRef ref = store.readPage(blankId)) {
            assertArrayEquals(v("page"), LeafPage.getValue(ref.bytes(), v("fresh")));
        }
    }

    @Test
    void handleCloseIsIdempotentAndReleasesLatch() throws Exception {
        store = new NodeStore(dbFile, 16);
        long rootId = store.rootId();

        PageWrite handle = store.latchForWrite(rootId);
        handle.close();
        handle.close();

        store.latchForWrite(rootId).close();
    }

    @Test
    void readerIsNotBlockedDuringWriteLatchWindow() throws Exception {
        store = new NodeStore(dbFile, 16);
        long rootId = store.rootId();
        try (PageWrite w = store.latchForWrite(rootId)) {
            LeafPage.put(w.bytes(), v("a"), v("1"));
            w.markDirty();
        }

        PageWrite w = store.latchForWrite(rootId);
        LeafPage.put(w.bytes(), v("b"), v("2"));
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<byte[][]> read = exec.submit(() -> {
                try (PageRef ref = store.readPage(rootId)) {
                    return new byte[][]{
                            LeafPage.getValue(ref.bytes(), v("a")),
                            LeafPage.getValue(ref.bytes(), v("b"))};
                }
            });
            byte[][] seen = read.get(5, TimeUnit.SECONDS);
            assertArrayEquals(v("1"), seen[0]);
            assertNull(seen[1], "unpublished shadow mutation leaked to a reader");
        } finally {
            exec.shutdown();
            w.close();
        }
    }

    @Test
    void markDirtyPublishesShadowBeforeClose() throws Exception {
        store = new NodeStore(dbFile, 16);
        long rootId = store.rootId();

        PageWrite w = store.latchForWrite(rootId);
        LeafPage.put(w.bytes(), v("k"), v("v"));
        w.markDirty();

        try (PageRef ref = store.readPage(rootId)) { // same thread: write lock not held mid-window
            assertArrayEquals(v("v"), LeafPage.getValue(ref.bytes(), v("k")));
        }
        w.close();
    }

    @Test
    void evictsPagesAndReloadsThemCorrectly() throws Exception {
        store = new NodeStore(dbFile, 8);
        BLinkTree tree = new BLinkTree(store);
        byte[] bigValue = new byte[4000];
        List<byte[]> keys = new ArrayList<>();
        int count = 150;

        for (int i = 0; i < count; i++) {
            byte[] key = v("key" + i);
            keys.add(key);
            tree.addValue(key, bigValue);
        }

        assertTrue(store.pool().getEvictions() > 0);
        for (int i = 0; i < count; i++) {
            assertArrayEquals(bigValue, tree.get(keys.get(i)), "wrong value for key " + i);
        }
    }

    @Test
    void closeFlushesDirtyPagesToDisk() throws IOException {
        store = new NodeStore(dbFile, 16);
        BLinkTree tree = new BLinkTree(store);
        tree.addValue(v("persistent"), v("yes"));
        store.close();
        store = null;

        NodeStore reopened = new NodeStore(dbFile, 16);
        try {
            BLinkTree reopenedTree = new BLinkTree(reopened);
            assertArrayEquals(v("yes"), reopenedTree.get(v("persistent")));
        } finally {
            reopened.close();
        }
    }

    @Test
    void throwsBufferExhaustedWhenPoolSaturatedByPins() throws Exception {
        store = new NodeStore(dbFile, 1);
        long rootId = store.rootId();

        PageWrite held = store.latchForWrite(rootId);

        assertThrows(BufferExhaustedException.class, () -> store.readPage(rootId + 1));

        held.close();
    }

    @Test
    void deleteRemovesKey() throws Exception {
        store = new NodeStore(dbFile, 16);
        BLinkTree tree = new BLinkTree(store);
        tree.addValue(v("a"), v("1"));
        tree.addValue(v("b"), v("2"));

        tree.delete(v("a"));

        assertNull(tree.get(v("a")));
        assertArrayEquals(v("2"), tree.get(v("b")));
    }

    @Test
    void deleteAbsentKeyIsNoop() throws Exception {
        store = new NodeStore(dbFile, 16);
        BLinkTree tree = new BLinkTree(store);
        tree.addValue(v("a"), v("1"));

        tree.delete(v("missing"));

        assertArrayEquals(v("1"), tree.get(v("a")));
        assertNull(tree.get(v("missing")));
    }

    @Test
    void deleteEmptiedLeafSelfHealsOnReinsert() throws Exception {
        store = new NodeStore(dbFile, 16);
        BLinkTree tree = new BLinkTree(store);
        tree.addValue(v("a"), v("1"));

        tree.delete(v("a"));
        assertNull(tree.get(v("a")));

        tree.addValue(v("a"), v("2"));

        assertArrayEquals(v("2"), tree.get(v("a")));
    }

    @Test
    void deleteAcrossSplitsLeavesRemainingKeysCorrect() throws Exception {
        store = new NodeStore(dbFile, 64);
        BLinkTree tree = new BLinkTree(store);
        byte[] bigValue = new byte[200];
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
                assertArrayEquals(bigValue, tree.get(v("key" + i)), "wrong value for key " + i);
            }
        }
    }

    @Test
    void concurrentInsertsAndDeletesStayConsistent() throws Exception {
        store = new NodeStore(dbFile, 128);
        BLinkTree tree = new BLinkTree(store);
        byte[] bigValue = new byte[200];
        int keyCount = 500;
        int threads = 8;

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
                    // each key is owned by exactly one thread, so the final state is deterministic
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
                f.get();
            }
        } finally {
            exec.shutdown();
        }

        for (int i = 0; i < keyCount; i++) {
            if (i % 2 == 0) {
                assertNull(tree.get(v("key" + i)), "deleted key " + i + " still present");
            } else {
                assertArrayEquals(bigValue, tree.get(v("key" + i)), "wrong value for key " + i);
            }
        }
    }
}
