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

    private static Value v(String s) {
        return new Value(s.getBytes());
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
            LeafPage.put(w.bytes(), v("a").bytes(), v("1").bytes());
            LeafPage.put(w.bytes(), v("c").bytes(), v("3").bytes());
            w.markDirty();
        }

        try (PageRef ref = store.readPage(rootId)) {
            assertEquals(v("1"), LeafPage.getValue(ref.bytes(), v("a").bytes()));
            assertEquals(v("3"), LeafPage.getValue(ref.bytes(), v("c").bytes()));
        }
    }

    @Test
    void latchForWriteBlankSkipsContentButPublishesFormattedPage() throws Exception {
        store = new NodeStore(dbFile, 16);
        long blankId = store.allocate();

        try (PageWrite w = store.latchForWriteBlank(blankId)) {
            LeafPage.formatEmpty(w.bytes(), 1, Pages.NO_LINK, null);
            LeafPage.put(w.bytes(), v("fresh").bytes(), v("page").bytes());
            w.markDirty();
        }

        try (PageRef ref = store.readPage(blankId)) {
            assertEquals(v("page"), LeafPage.getValue(ref.bytes(), v("fresh").bytes()));
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
            LeafPage.put(w.bytes(), v("a").bytes(), v("1").bytes());
            w.markDirty();
        }

        PageWrite w = store.latchForWrite(rootId);
        LeafPage.put(w.bytes(), v("b").bytes(), v("2").bytes());
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<Value[]> read = exec.submit(() -> {
                try (PageRef ref = store.readPage(rootId)) {
                    return new Value[]{
                            LeafPage.getValue(ref.bytes(), v("a").bytes()),
                            LeafPage.getValue(ref.bytes(), v("b").bytes())};
                }
            });
            Value[] seen = read.get(5, TimeUnit.SECONDS);
            assertEquals(v("1"), seen[0]);
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
        LeafPage.put(w.bytes(), v("k").bytes(), v("v").bytes());
        w.markDirty();

        try (PageRef ref = store.readPage(rootId)) { // same thread: write lock not held mid-window
            assertEquals(v("v"), LeafPage.getValue(ref.bytes(), v("k").bytes()));
        }
        w.close();
    }

    @Test
    void evictsPagesAndReloadsThemCorrectly() throws Exception {
        store = new NodeStore(dbFile, 8);
        BLinkTree tree = new BLinkTree(store);
        byte[] bigValue = new byte[4000];
        List<Value> keys = new ArrayList<>();
        int count = 150;

        for (int i = 0; i < count; i++) {
            Value key = v("key" + i);
            keys.add(key);
            tree.addValue(key, new Value(bigValue));
        }

        assertTrue(store.pool().getEvictions() > 0);
        for (int i = 0; i < count; i++) {
            assertEquals(new Value(bigValue), tree.get(keys.get(i)), "wrong value for key " + i);
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
            assertEquals(v("yes"), reopenedTree.get(v("persistent")));
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
        assertEquals(v("2"), tree.get(v("b")));
    }

    @Test
    void deleteAbsentKeyIsNoop() throws Exception {
        store = new NodeStore(dbFile, 16);
        BLinkTree tree = new BLinkTree(store);
        tree.addValue(v("a"), v("1"));

        tree.delete(v("missing"));

        assertEquals(v("1"), tree.get(v("a")));
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

        assertEquals(v("2"), tree.get(v("a")));
    }

    @Test
    void deleteAcrossSplitsLeavesRemainingKeysCorrect() throws Exception {
        store = new NodeStore(dbFile, 64);
        BLinkTree tree = new BLinkTree(store);
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
        store = new NodeStore(dbFile, 128);
        BLinkTree tree = new BLinkTree(store);
        Value bigValue = new Value(new byte[200]);
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
                assertEquals(bigValue, tree.get(v("key" + i)), "wrong value for key " + i);
            }
        }
    }
}
