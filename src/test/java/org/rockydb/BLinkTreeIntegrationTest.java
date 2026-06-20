package org.rockydb;

import org.junit.jupiter.api.AfterEach;
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

    private BufferedPool pool;
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
        pool = new BufferedPool(db, 1000);
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
}
