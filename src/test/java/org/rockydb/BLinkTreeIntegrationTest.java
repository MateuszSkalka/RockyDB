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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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

    private static byte[] keyOf(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(id);
        return buffer.array();
    }

    private static byte[] valueOf(long id) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(id);
        buffer.putLong(~id);
        return buffer.array();
    }

    @Test
    void countIsExactAcrossSplitsAndPersistsAcrossReopen() throws IOException {
        File db = tempDir.resolve("count-reopen.db").toFile();
        try (NodeStore store = new NodeStore(db, 64)) {
            BLinkTree t = new BLinkTree(store);
            assertEquals(0, t.count());
            for (long i = 0; i < 500; i++) {
                t.addValue(keyOf(i), valueOf(i));
            }
            assertEquals(500, t.count());
            for (long i = 0; i < 500; i++) {
                t.addValue(keyOf(i), valueOf(~i));
            }
            assertEquals(500, t.count());
        }
        try (NodeStore store = new NodeStore(db, 64)) {
            BLinkTree t = new BLinkTree(store);
            assertEquals(500, t.count());
            for (long i = 0; i < 400; i++) {
                t.delete(keyOf(i));
            }
            assertEquals(100, t.count());
        }
        try (NodeStore store = new NodeStore(db, 64)) {
            assertEquals(100, new BLinkTree(store).count());
        }
    }

    @Test
    void entriesScansWholeTreeInKeyOrderAcrossSplitsAndMerges() throws IOException {
        File db = tempDir.resolve("scan.db").toFile();
        try (NodeStore store = new NodeStore(db, 64)) {
            BLinkTree t = new BLinkTree(store);
            assertFalse(t.entries().hasNext());

            for (long i = 0; i < 500; i++) {
                t.addValue(keyOf(i), valueOf(i));
            }
            List<BLinkTree.Entry> all = new ArrayList<>();
            t.entries().forEachRemaining(all::add);
            assertEquals(500, all.size());
            assertEquals(t.count(), all.size());
            for (int i = 0; i < 500; i++) {
                assertArrayEquals(keyOf(i), all.get(i).key(), "key at position " + i);
                assertArrayEquals(valueOf(i), all.get(i).value(), "value at position " + i);
            }

            for (long i = 0; i < 400; i++) {
                t.delete(keyOf(i));
            }
            List<BLinkTree.Entry> rest = new ArrayList<>();
            t.entries().forEachRemaining(rest::add);
            assertEquals(100, rest.size());
            for (int i = 0; i < 100; i++) {
                assertArrayEquals(keyOf(400 + i), rest.get(i).key(), "key at position " + i);
            }
        }
    }

    @Test
    void entriesStreamsLazilyWhileWritesContinue() throws IOException {
        File db = tempDir.resolve("scan-lazy.db").toFile();
        try (NodeStore store = new NodeStore(db, 64)) {
            BLinkTree t = new BLinkTree(store);
            for (long i = 0; i < 500; i++) {
                t.addValue(keyOf(i), valueOf(i));
            }

            Iterator<BLinkTree.Entry> it = t.entries();
            for (int i = 0; i < 10; i++) {
                assertTrue(it.hasNext());
                assertArrayEquals(keyOf(i), it.next().key());
            }
            // the lazy scan picks up keys inserted ahead of its position while it holds its epoch ticket
            for (long i = 500; i < 600; i++) {
                t.addValue(keyOf(i), valueOf(i));
            }

            int seen = 10;
            long prev = 9;
            while (it.hasNext()) {
                long k = ByteBuffer.wrap(it.next().key()).getLong();
                assertEquals(prev + 1, k, "scan must stay strictly ascending, no duplicates");
                prev = k;
                seen++;
            }
            assertEquals(600, seen);
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void concurrentInsertsAreAllRetrievable(int threads, int elementsPerThread, int insertsPerThread) throws Exception {
        openTree(threads, elementsPerThread, insertsPerThread);
        Map<Long, byte[]> expected = new ConcurrentHashMap<>();

        runWriters(threads, elementsPerThread, insertsPerThread, expected, new AtomicInteger());

        for (Map.Entry<Long, byte[]> entry : expected.entrySet()) {
            byte[] got = tree.get(keyOf(entry.getKey()));
            assertArrayEquals(entry.getValue(), got, "wrong value for key " + entry.getKey());
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
            byte[] got = tree.get(keyOf(entry.getKey()));
            assertArrayEquals(entry.getValue(), got, "wrong value for key " + entry.getKey());
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
                byte[] value = valueOf(keyId);
                tree.addValue(keyOf(keyId), value);
                expected.put(keyId, value);
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
            assertArrayEquals(valueOf(i), tree.get(keyOf(i)), "surviving key missing: " + i);
        }
        for (int i = 0; i < deletedUpTo; i++) {
            assertNull(tree.get(keyOf(i)), "deleted key still present: " + i);
        }
        for (int i = 0; i < 300; i++) {
            tree.addValue(keyOf(i), valueOf(i));
        }
        for (int i = 0; i < 300; i++) {
            assertArrayEquals(valueOf(i), tree.get(keyOf(i)), "re-inserted key missing: " + i);
        }
    }

    @Test
    void deleteCascadesThroughInternalNodesAndStaysConsistent() throws Exception {
        openTree(1, 0, 0);
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
            assertArrayEquals(largeValue(i, payloadLen), tree.get(largeKey(i, payloadLen)), "surviving key missing: " + i);
        }
        for (int i = 0; i < deletedUpTo; i++) {
            assertNull(tree.get(largeKey(i, payloadLen)), "deleted key still present: " + i);
        }
        for (int i = 0; i < deletedUpTo; i++) {
            tree.addValue(largeKey(i, payloadLen), largeValue(i, payloadLen));
        }
        for (int i = 0; i < total; i++) {
            assertArrayEquals(largeValue(i, payloadLen), tree.get(largeKey(i, payloadLen)), "re-inserted key missing: " + i);
        }
    }

    @Test
    void concurrentLargeValueMergesKeepTreeConsistent() throws Exception {
        openTree(1, 0, 0);
        int payloadLen = 1500;
        int threads = 6;
        int keysPerThread = 50;
        int keySpace = threads * keysPerThread;
        ConcurrentHashMap<Long, byte[]> present = new ConcurrentHashMap<>();
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
                        long base = (long) threadId * keysPerThread;
                        for (int op = 0; op < 1500; op++) {
                            int k = (int) (base + rnd.nextLong(keysPerThread));
                            byte[] key = largeKey(k, payloadLen);
                            byte[] val = largeValue(k, payloadLen);
                            if (rnd.nextInt(3) < 2) {
                                tree.addValue(key, val);
                                present.put((long) k, val);
                            } else {
                                tree.delete(key);
                                present.remove((long) k);
                            }
                            byte[] got = tree.get(key);
                            if (got != null && !Arrays.equals(got, val)) {
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
            byte[] got = tree.get(largeKey((int) k, payloadLen));
            if (present.containsKey(k)) {
                assertArrayEquals(largeValue((int) k, payloadLen), got, "present key missing: " + k);
            } else {
                assertNull(got, "absent key still present: " + k);
            }
        }
    }

    private static byte[] largeKey(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 1;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return b;
    }

    private static byte[] largeValue(int id, int len) {
        byte[] b = new byte[len];
        b[0] = (byte) 2;
        b[len - 1] = (byte) (id & 0xFF);
        b[len - 2] = (byte) ((id >> 8) & 0xFF);
        return b;
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void concurrentInsertsAndDeletesStayConsistent(int threads, int elementsPerThread, int insertsPerThread) throws Exception {
        openTree(threads, elementsPerThread, insertsPerThread);
        int keySpace = 1000;
        ConcurrentHashMap<Long, byte[]> present = new ConcurrentHashMap<>();
        for (int i = 0; i < keySpace; i++) {
            tree.addValue(keyOf(i), valueOf(i));
            present.put((long) i, valueOf(i));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);
        Object[] keyStripes = new Object[64];
        for (int i = 0; i < keyStripes.length; i++) {
            keyStripes[i] = new Object();
        }
        try {
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                executor.submit(() -> {
                    try {
                        Random rnd = new Random(5_000L + threadId);
                        for (int op = 0; op < insertsPerThread; op++) {
                            long k = rnd.nextLong(keySpace);
                            // stripe-serialize the tree op + model update so the model
                            // cannot invert two concurrent ops on the same key
                            synchronized (keyStripes[(int) (k % keyStripes.length)]) {
                                if (rnd.nextBoolean()) {
                                    tree.addValue(keyOf(k), valueOf(k));
                                    present.put(k, valueOf(k));
                                } else {
                                    tree.delete(keyOf(k));
                                    present.remove(k);
                                }
                            }
                            byte[] got = tree.get(keyOf(k));
                            if (got != null && !Arrays.equals(got, valueOf(k))) {
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
            byte[] got = tree.get(keyOf(k));
            if (present.containsKey(k)) {
                assertArrayEquals(valueOf(k), got, "present key missing after shutdown: " + k);
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

        for (int i = 0; i < keys; i++) {
            assertArrayEquals(largeValue(i, payloadLen), tree.get(largeKey(i, payloadLen)),
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
            for (int i = 0; i < 60; i++) {
                t.delete(largeKey(i, payloadLen));
            }
        }

        try (NodeStore p = new NodeStore(db, 64)) {
            long hwmAfter = p.highWaterMark();
            assertEquals(hwmBefore, hwmAfter, "reopen must not change the high-water mark");
            long ticket = p.enterOp();
            try {
                long firstAlloc = p.allocate();
                assertTrue(firstAlloc < hwmAfter,
                        "first allocation after restart should reuse a recycled page (" + firstAlloc
                                + " < hwm " + hwmAfter + ")");
            } finally {
                p.exitOp(ticket);
            }
        }
    }
}
