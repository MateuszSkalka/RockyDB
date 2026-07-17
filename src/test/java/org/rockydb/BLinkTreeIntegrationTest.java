package org.rockydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 120)
class BLinkTreeIntegrationTest {

    private static final long PARTITION_OFFSET = 1_000_000L;

    @TempDir
    Path tempDir;

    private NodeStore pool;
    private BLinkTree tree;

    static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(1, 200, 200),
                Arguments.of(4, 300, 300),
                Arguments.of(8, 200, 400)
        );
    }

    private void openTree(int threads, int elementsPerThread, int insertsPerThread) throws IOException {
        File db = tempDir.resolve("it-" + threads + "-" + elementsPerThread + "-" + insertsPerThread + ".db").toFile();
        pool = new NodeStore(db, 1000);
        tree = new BLinkTree(pool);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (pool != null) {
            pool.close();
            pool = null;
        }
    }

    private static Value keyOf(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(id);
        return new Value(buffer.array());
    }

    private static Value valueOf(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(id);
        buffer.putLong(~id);
        return new Value(buffer.array());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void concurrentInsertsAreAllRetrievable(int threads, int elementsPerThread, int insertsPerThread) throws Exception {
        openTree(threads, elementsPerThread, insertsPerThread);
        Map<Long, byte[]> expected = new ConcurrentHashMap<>();

        runWriters(threads, elementsPerThread, insertsPerThread, expected, new AtomicInteger());

        for (Map.Entry<Long, byte[]> entry : expected.entrySet()) {
            Value got = tree.get(keyOf(entry.getKey()));
            assertEquals(new Value(entry.getValue()), got, "wrong value for key " + entry.getKey());
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void mixedInsertsAndGetsRunWithoutCorruption(int threads, int elementsPerThread, int insertsPerThread) throws Exception {
        openTree(threads, elementsPerThread, insertsPerThread);
        Map<Long, byte[]> expected = new ConcurrentHashMap<>();
        AtomicInteger writerErrors = new AtomicInteger();
        AtomicInteger readerErrors = new AtomicInteger();
        int writers = Math.max(1, threads / 2);
        int readers = Math.max(1, threads - writers);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch writersDone = new CountDownLatch(writers);
        try {
            for (int t = 0; t < writers; t++) {
                int threadId = t;
                executor.submit(() -> writeUntilDone(threadId, elementsPerThread, insertsPerThread, expected, writerErrors, writersDone));
            }
            for (int t = 0; t < readers; t++) {
                int threadId = t;
                executor.submit(() -> readUntilWritersDone(threadId, elementsPerThread, readerErrors, writersDone));
            }

            assertTrue(writersDone.await(90, TimeUnit.SECONDS), "writers did not finish in time");
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "executor did not terminate");
        }

        assertEquals(0, writerErrors.get(), "writers reported errors");
        assertEquals(0, readerErrors.get(), "readers reported errors");

        for (Map.Entry<Long, byte[]> entry : expected.entrySet()) {
            Value got = tree.get(keyOf(entry.getKey()));
            assertEquals(new Value(entry.getValue()), got, "wrong value for key " + entry.getKey());
        }
    }

    private void runWriters(int threads, int elementsPerThread, int insertsPerThread,
                            Map<Long, byte[]> expected, AtomicInteger errors) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                executor.submit(() -> writeAll(threadId, elementsPerThread, insertsPerThread, expected, errors, done));
            }
            assertTrue(done.await(90, TimeUnit.SECONDS), "writers did not finish in time");
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "executor did not terminate");
        }
        assertEquals(0, errors.get(), "writers reported errors");
    }

    private void writeAll(int threadId, int elementsPerThread, int insertsPerThread,
                          Map<Long, byte[]> expected, AtomicInteger errors, CountDownLatch done) {
        try {
            Random rnd = new Random(1_000L + threadId);
            long base = (long) threadId * PARTITION_OFFSET;
            for (int j = 0; j < insertsPerThread; j++) {
                long keyId = base + rnd.nextLong(elementsPerThread);
                Value value = valueOf(keyId);
                tree.addValue(keyOf(keyId), value);
                expected.put(keyId, value.bytes());
            }
        } catch (Exception e) {
            errors.incrementAndGet();
        } finally {
            done.countDown();
        }
    }

    private void writeUntilDone(int threadId, int elementsPerThread, int insertsPerThread,
                                Map<Long, byte[]> expected, AtomicInteger errors, CountDownLatch done) {
        writeAll(threadId, elementsPerThread, insertsPerThread, expected, errors, done);
    }

    private void readUntilWritersDone(int threadId, int elementsPerThread,
                                      AtomicInteger errors, CountDownLatch writersDone) {
        Random rnd = new Random(9_000L + threadId);
        long base = (long) threadId * PARTITION_OFFSET;
        while (writersDone.getCount() > 0) {
            long keyId = base + rnd.nextLong(elementsPerThread);
            try {
                tree.get(keyOf(keyId));
            } catch (Exception e) {
                errors.incrementAndGet();
            }
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }

    @Test
    void deleteThenMergeKeepsRemainingKeysConsistent() throws Exception {
        openTree(1, 0, 0);
        int total = 2000;
        for (int i = 0; i < total; i++) {
            tree.addValue(keyOf(i), valueOf(i));
        }
        int deletedUpTo = 1500;
        for (int i = 0; i < deletedUpTo; i++) {
            tree.delete(keyOf(i));
        }
        for (int i = deletedUpTo; i < total; i++) {
            assertEquals(new Value(valueOf(i).bytes()), tree.get(keyOf(i)), "surviving key missing: " + i);
        }
        for (int i = 0; i < deletedUpTo; i++) {
            assertNull(tree.get(keyOf(i)), "deleted key still present: " + i);
        }
        // re-inserting into a tombstoned/merged region must still work (descent follows redirects)
        for (int i = 0; i < 300; i++) {
            tree.addValue(keyOf(i), valueOf(i));
        }
        for (int i = 0; i < 300; i++) {
            assertEquals(new Value(valueOf(i).bytes()), tree.get(keyOf(i)), "re-inserted key missing: " + i);
        }
    }

    @Test
    void deleteCascadesThroughInternalNodesAndStaysConsistent() throws Exception {
        openTree(1, 0, 0);
        // Large keys+values so the tree reaches height 3 quickly and branch nodes underflow on delete,
        // exercising the internal-node merge cascade (not just leaf merges).
        int total = 120;
        int payloadLen = 2000;
        for (int i = 0; i < total; i++) {
            tree.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
        }
        int deletedUpTo = 100;
        for (int i = 0; i < deletedUpTo; i++) {
            tree.delete(largeKey(i, payloadLen));
        }
        for (int i = deletedUpTo; i < total; i++) {
            assertEquals(largeValue(i, payloadLen), tree.get(largeKey(i, payloadLen)), "surviving key missing: " + i);
        }
        for (int i = 0; i < deletedUpTo; i++) {
            assertNull(tree.get(largeKey(i, payloadLen)), "deleted key still present: " + i);
        }
        // re-insert into tombstoned/merged regions (descent + ascent must follow redirects)
        for (int i = 0; i < deletedUpTo; i++) {
            tree.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
        }
        for (int i = 0; i < total; i++) {
            assertEquals(largeValue(i, payloadLen), tree.get(largeKey(i, payloadLen)), "re-inserted key missing: " + i);
        }
    }

    @Test
    void concurrentLargeValueMergesKeepTreeConsistent() throws Exception {
        openTree(1, 0, 0);
        int payloadLen = 1500;
        int threads = 6;
        int keysPerThread = 50;
        int keySpace = threads * keysPerThread;
        // pre-populate a deep tree (height >= 3) so deletes cascade through internal nodes
        ConcurrentHashMap<Long, Value> present = new ConcurrentHashMap<>();
        for (int i = 0; i < keySpace; i++) {
            tree.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
            present.put((long) i, largeValue(i, payloadLen));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                executor.submit(() -> {
                    try {
                        Random rnd = new Random(7_000L + threadId);
                        long base = (long) threadId * keysPerThread; // disjoint keys per thread -> clean oracle
                        for (int op = 0; op < 1500; op++) {
                            int k = (int) (base + rnd.nextLong(keysPerThread));
                            Value key = largeKey(k, payloadLen);
                            Value val = largeValue(k, payloadLen);
                            if (rnd.nextInt(3) < 2) {
                                tree.addValue(key, val);
                                present.put((long) k, val);
                            } else {
                                tree.delete(key);
                                present.remove((long) k);
                            }
                            // strong invariant: a key's value, when present, is always its deterministic value
                            Value got = tree.get(key);
                            if (got != null && !got.equals(val)) {
                                errors.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(90, TimeUnit.SECONDS), "workers did not finish in time");
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "executor did not terminate");
        }

        assertEquals(0, errors.get(), "corruption/exception during concurrent large-value merges");
        for (long k = 0; k < keySpace; k++) {
            Value got = tree.get(largeKey((int) k, payloadLen));
            if (present.containsKey(k)) {
                assertEquals(largeValue((int) k, payloadLen), got, "present key missing: " + k);
            } else {
                assertNull(got, "absent key still present: " + k);
            }
        }
    }




    private static Value largeKey(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 1;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return new Value(b);
    }

    private static Value largeValue(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 2;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return new Value(b);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void concurrentInsertsAndDeletesStayConsistent(int threads, int elementsPerThread, int insertsPerThread) throws Exception {
        openTree(threads, elementsPerThread, insertsPerThread);
        int keySpace = 1000;
        ConcurrentHashMap<Long, Value> present = new ConcurrentHashMap<>();
        for (int i = 0; i < keySpace; i++) {
            tree.addValue(keyOf(i), valueOf(i));
            present.put((long) i, valueOf(i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                executor.submit(() -> {
                    try {
                        Random rnd = new Random(5_000L + threadId);
                        for (int op = 0; op < insertsPerThread; op++) {
                            long k = rnd.nextLong(keySpace);
                            if (rnd.nextBoolean()) {
                                tree.addValue(keyOf(k), valueOf(k));
                                present.put(k, valueOf(k));
                            } else {
                                tree.delete(keyOf(k));
                                present.remove(k);
                            }
                            // strong invariant: a key's value, when present, is always valueOf(k)
                            Value got = tree.get(keyOf(k));
                            if (got != null && !got.equals(valueOf(k))) {
                                errors.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(90, TimeUnit.SECONDS), "workers did not finish in time");
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "executor did not terminate");
        }

        assertEquals(0, errors.get(), "corruption detected during mixed insert/delete workload");
        for (long k = 0; k < keySpace; k++) {
            Value got = tree.get(keyOf(k));
            if (present.containsKey(k)) {
                assertEquals(valueOf(k), got, "present key missing after shutdown: " + k);
            } else {
                assertNull(got, "absent key still present after shutdown: " + k);
            }
        }
    }

    @Test
    void retiredPagesAreRecycledInsteadOfGrowingTheFile() throws Exception {
        openTree(1, 0, 0);
        int payloadLen = 1500;
        int keys = 60;
        // Build a deep tree and remember how high the file grew to hold it.
        for (int i = 0; i < keys; i++) {
            tree.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
        }
        long baselineHwm = pool.highWaterMark();

        for (int cycle = 0; cycle < 30; cycle++) {
            for (int i = 0; i < keys; i++) {
                tree.delete(largeKey(i, payloadLen));
            }
            for (int i = 0; i < keys; i++) {
                tree.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
            }
        }

        long finalHwm = pool.highWaterMark();

        long allowedGrowth = baselineHwm + (baselineHwm / 4) + 32;
        assertTrue(finalHwm <= allowedGrowth,
                "file grew from " + baselineHwm + " to " + finalHwm
                        + " pages; expected recycling to bound growth");

        // And every key is still retrievable — recycling did not corrupt the tree.
        for (int i = 0; i < keys; i++) {
            assertEquals(largeValue(i, payloadLen), tree.get(largeKey(i, payloadLen)),
                    "key missing after churn: " + i);
        }
    }

    @Test
    void freelistSurvivesRestartAndIsReused() throws Exception {
        File db = tempDir.resolve("restart.db").toFile();
        long hwmBefore;
        try (NodeStore p = new NodeStore(db, 64)) {
            BLinkTree t = new BLinkTree(p);
            int payloadLen = 1500;
            for (int i = 0; i < 60; i++) {
                t.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
            }
            hwmBefore = p.highWaterMark();
            // Delete ALL keys: forces leaf merges, branch collapses, and root collapse — each
            // tombstoning a page that gets retired into the free-log. With everything deleted the
            // tree shrinks to a single empty leaf, so many pages are guaranteed to be retired.
            for (int i = 0; i < 60; i++) {
                t.delete(largeKey(i, payloadLen));
            }
            // pool.close() flushes dirty data pages, persists the free-log pages (written via raw IO),
            // and persists the epoch counter.
        }

        // Reopen: the persisted epoch is bumped above the old current on reopen, so retired pages
        // become immediately epoch-safe. The free-log's head/tail + page contents survived on disc,
        // so the first allocation must recycle a retired page rather than grow the file.
        try (NodeStore p = new NodeStore(db, 64)) {
            long hwmAfter = p.highWaterMark();
            assertEquals(hwmBefore, hwmAfter, "reopen must not change the high-water mark");
            long ticket = p.enterOp();
            try {
                long firstAlloc = p.allocator().allocate();
                assertTrue(firstAlloc < hwmAfter,
                        "first allocation after restart should reuse a recycled page (" + firstAlloc
                                + " < hwm " + hwmAfter + ")");
            } finally {
                p.exitOp(ticket);
            }
        }
    }
}
