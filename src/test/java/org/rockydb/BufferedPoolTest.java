package org.rockydb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedPoolTest {

    private static final String TEST_DB_PATH = "test.db";
    private BufferedPool pool;
    private BLinkTree tree;

    @Before
    public void setUp() throws IOException {
        // Clean up any existing test database
        File dbFile = new File(TEST_DB_PATH);
        if (dbFile.exists()) {
            dbFile.delete();
        }

        // Create BufferedPool with small size for testing
        pool = new BufferedPool(new File(TEST_DB_PATH), 16);
        tree = new BLinkTree(pool);
    }

    @After
    public void tearDown() throws Exception {
        if (pool != null) {
            pool.close();
        }
        File dbFile = new File(TEST_DB_PATH);
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    public void testBasicOperations() {
        // Test basic get and add operations
        Value key1 = new Value("key1".getBytes());
        Value value1 = new Value("value1".getBytes());

        // Add a value
        tree.addValue(key1, value1);

        // Retrieve it
        Value retrieved = tree.get(key1);
        assertEquals(value1, retrieved);

        // Update the value
        Value newValue1 = new Value("newvalue1".getBytes());
        tree.addValue(key1, newValue1);
        retrieved = tree.get(key1);
        assertEquals(newValue1, retrieved);
    }

    @Test
    public void testMultipleValues() {
        // Add multiple values and verify they can be retrieved
        int numValues = 100;
        for (int i = 0; i < numValues; i++) {
            Value key = new Value(("key" + i).getBytes());
            Value value = new Value(("value" + i).getBytes());
            tree.addValue(key, value);
        }

        // Verify all values can be retrieved
        for (int i = 0; i < numValues; i++) {
            Value key = new Value(("key" + i).getBytes());
            Value expected = new Value(("value" + i).getBytes());
            Value actual = tree.get(key);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testBufferPoolStatistics() {
        // Perform some operations and check statistics
        int numValues = 50;
        for (int i = 0; i < numValues; i++) {
            Value key = new Value(("key" + i).getBytes());
            Value value = new Value(("value" + i).getBytes());
            tree.addValue(key, value);
        }

        // Read back some values to generate hits
        for (int i = 0; i < numValues; i += 2) {
            Value key = new Value(("key" + i).getBytes());
            tree.get(key);
        }

        // Check statistics
        int hits = pool.getBufferHits();
        int misses = pool.getBufferMisses();
        int usedFrames = pool.getUsedFrames();

        assertTrue("Should have some buffer activity", hits + misses > 0);
        assertTrue("Should use some frames", usedFrames > 0);
        assertTrue("Used frames should not exceed total", usedFrames <= pool.getNumFrames());

        System.out.println("Buffer hit ratio: " + pool.getHitRatio());
        System.out.println("Used frames: " + usedFrames + "/" + pool.getNumFrames());
    }

    @Test
    public void testClockEviction() throws IOException {
        // Values large enough that each leaf holds only one entry, so 20 keys spread across many
        // pages and genuinely exceed the 4-frame pool. (With small values all 20 keys would fit a
        // single leaf and no eviction would ever occur, regardless of the pool size.)
        String largeValue = "x".repeat(5000);

        // Create a pool with very small size to force eviction
        try (BufferedPool smallPool = new BufferedPool(new File("test2.db"), 4)) {
            BLinkTree smallTree = new BLinkTree(smallPool);

            // Add enough values to force multiple evictions
            for (int i = 0; i < 20; i++) {
                Value key = new Value(("evictkey" + i).getBytes());
                Value value = new Value((largeValue + i).getBytes());
                smallTree.addValue(key, value);
            }

            // Verify evictions occurred
            assertTrue("Should have evicted some pages", smallPool.getEvictions() > 0);
            assertTrue("All frames should be used", smallPool.getUsedFrames() <= smallPool.getNumFrames());

            // Verify data integrity after evictions
            for (int i = 0; i < 20; i++) {
                Value key = new Value(("evictkey" + i).getBytes());
                Value expected = new Value((largeValue + i).getBytes());
                Value actual = smallTree.get(key);
                assertEquals("Data should be correct after eviction", expected, actual);
            }

            System.out.println("Evictions: " + smallPool.getEvictions());
        } finally {
            new File("test2.db").delete();
        }
    }

    @Test
    public void testPersistence() throws Exception {
        // Write some data
        for (int i = 0; i < 50; i++) {
            Value key = new Value(("persist" + i).getBytes());
            Value value = new Value(("persistvalue" + i).getBytes());
            tree.addValue(key, value);
        }

        // Close and reopen
        pool.close();
        pool = new BufferedPool(new File(TEST_DB_PATH), 16);
        tree = new BLinkTree(pool);

        // Verify data persisted
        for (int i = 0; i < 50; i++) {
            Value key = new Value(("persist" + i).getBytes());
            Value expected = new Value(("persistvalue" + i).getBytes());
            Value actual = tree.get(key);
            assertEquals("Data should persist across restarts", expected, actual);
        }
    }

    @Test
    public void testConcurrentReads() throws InterruptedException {
        // Add initial data
        for (int i = 0; i < 100; i++) {
            Value key = new Value(("concurrent" + i).getBytes());
            Value value = new Value(("value" + i).getBytes());
            tree.addValue(key, value);
        }

        int numThreads = 10;
        int readsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < readsPerThread; i++) {
                        int keyIdx = i % 100;
                        Value key = new Value(("concurrent" + keyIdx).getBytes());
                        Value expected = new Value(("value" + keyIdx).getBytes());
                        Value actual = tree.get(key);
                        if (!expected.equals(actual)) {
                            errors.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Concurrent reads should complete", latch.await(30, TimeUnit.SECONDS));
        assertEquals("No read errors should occur", 0, errors.get());

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testConcurrentWrites() throws InterruptedException {
        int numThreads = 5;
        int writesPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<List<KeyValuePair>> writtenValues = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
            List<KeyValuePair> threadValues = new ArrayList<>();
            writtenValues.add(threadValues);

            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < writesPerThread; i++) {
                        String keyStr = "thread" + threadId + "_key" + i;
                        String valueStr = "thread" + threadId + "_value" + i;
                        Value key = new Value(keyStr.getBytes());
                        Value value = new Value(valueStr.getBytes());
                        tree.addValue(key, value);
                        threadValues.add(new KeyValuePair(key, value));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Concurrent writes should complete", latch.await(30, TimeUnit.SECONDS));

        // Verify all written values can be read
        for (List<KeyValuePair> threadValues : writtenValues) {
            for (KeyValuePair kvp : threadValues) {
                Value actual = tree.get(kvp.key());
                assertEquals("Concurrently written value should be correct", kvp.value(), actual);
            }
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testFlushOnClose() throws Exception {
        // Write data and verify dirty frames
        for (int i = 0; i < 20; i++) {
            Value key = new Value(("flush" + i).getBytes());
            Value value = new Value(("flushvalue" + i).getBytes());
            tree.addValue(key, value);
        }

        int dirtyFramesBefore = pool.getDirtyFrames();
        assertTrue("Should have some dirty frames", dirtyFramesBefore > 0);

        // Close the pool (should flush dirty pages)
        pool.close();

        // Reopen and verify data persisted
        pool = new BufferedPool(new File(TEST_DB_PATH), 16);
        tree = new BLinkTree(pool);

        for (int i = 0; i < 20; i++) {
            Value key = new Value(("flush" + i).getBytes());
            Value expected = new Value(("flushvalue" + i).getBytes());
            Value actual = tree.get(key);
            assertEquals("Data should be flushed and persist", expected, actual);
        }
    }

    @Test
    public void testValueNotFound() {
        Value nonExistentKey = new Value("nonexistent".getBytes());
        Value result = tree.get(nonExistentKey);
        assertNull("Should return null for non-existent key", result);
    }

    @Test
    public void testLargeValues() {
        // Test with larger values to stress the buffer pool. Values must fit within a single
        // page: a leaf cell needs 16 + key.length + value.length <= MAX_NODE_SIZE (8187), so a
        // value up to ~8160 bytes fits. 7000 bytes is comfortably within that bound.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 700; i++) {
            sb.append("abcdefghij");
        }
        String largeValueStr = sb.toString();

        for (int i = 0; i < 10; i++) {
            Value key = new Value(("largekey" + i).getBytes());
            Value value = new Value(largeValueStr.getBytes());
            tree.addValue(key, value);
        }

        // Verify
        for (int i = 0; i < 10; i++) {
            Value key = new Value(("largekey" + i).getBytes());
            Value expected = new Value(largeValueStr.getBytes());
            Value actual = tree.get(key);
            assertEquals("Large values should be stored correctly", expected, actual);
        }
    }

    @Test
    public void testFramePinUnderflowGuard() {
        // A pin released more often than acquired is a bug. It must surface immediately (rather
        // than silently driving pinCount negative and permanently disabling eviction), and the
        // frame must remain reclaimable afterwards.
        Frame frame = new Frame(0);
        frame.pin();
        frame.unpin();
        try {
            frame.unpin(); // no matching pin outstanding
            fail("Expected IllegalStateException on pin count underflow");
        } catch (IllegalStateException expected) {
            assertTrue("message should name the underflow",
                    expected.getMessage().contains("underflow"));
        }
        assertTrue("frame should be reclaimable after underflow recovery", frame.tryClaim());
        frame.releaseClaim();
    }

    @Test
    public void testConcurrentSplitsAndRootGrowth() throws Exception {
        // Enough threads and keys, with values large enough to force frequent leaf splits and a
        // multi-level tree, written concurrently into a small pool. This stresses split
        // propagation, root splits, eviction under contention, and the re-descent fallback in
        // addValue (ancestors exhausted when the tree grows deeper mid-insert). Correctness is
        // checked by reading kback every value once the writers have quiesced.
        int numThreads = 8;
        int keysPerThread = 400;
        String valueBody = "v".repeat(2000);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<List<KeyValuePair>> written = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            written.add(new ArrayList<>());
        }
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < keysPerThread; i++) {
                        // Disjoint keys per thread (threadId is the single-char prefix), so every
                        // insert is a fresh key, never an upsert.
                        Value key = new Value((threadId + "_" + i).getBytes());
                        Value value = new Value((valueBody + "_" + threadId + "_" + i).getBytes());
                        tree.addValue(key, value);
                        written.get(threadId).add(new KeyValuePair(key, value));
                    }
                } catch (Throwable e) {
                    errors.add(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("concurrent inserts should complete", latch.await(60, TimeUnit.SECONDS));
        if (!errors.isEmpty()) {
            // Surface the swallowed exception so a writer abort is diagnosed, not masked.
            errors.get(0).printStackTrace();
            fail("insert threw: " + errors.get(0));
        }

        int verified = 0;
        for (List<KeyValuePair> threadValues : written) {
            for (KeyValuePair kvp : threadValues) {
                assertEquals("value must survive concurrent splits and root growth",
                        kvp.value(), tree.get(kvp.key()));
                verified++;
            }
        }
        assertEquals("every inserted key must be retrievable",
                numThreads * keysPerThread, verified);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    // Helper class for concurrent write test
    private record KeyValuePair(Value key, Value value) {}
}
