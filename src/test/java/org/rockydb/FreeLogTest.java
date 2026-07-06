package org.rockydb;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class FreeLogTest {

    private TestIO io;
    private FreeLog freeLog;
    private long bootstrapHead;
    private long bootstrapTail;

    /**
     * A self-contained FreeLogIO backed by an in-memory map so tests isolate FreeLog's logic from the
     * pool's PageCodec-shaped IO surface.
     */
    private final class TestIO implements FreeLog.FreeLogIO {
        private final java.util.HashMap<Long, byte[]> pages = new java.util.HashMap<>();
        private final AtomicLong next = new AtomicLong(1);
        private final AtomicLong epoch = new AtomicLong(0);

        @Override public ByteBuffer readPage(long pageId) {
            byte[] b = pages.get(pageId);
            if (b == null) {
                // lazy-create for tests (a fresh allocateRaw page is empty already)
                b = new byte[Store.PAGE_SIZE];
                pages.put(pageId, b);
            }
            return ByteBuffer.wrap(b);
        }

        @Override public void writePage(long pageId, ByteBuffer buffer) {
            byte[] copy = new byte[Store.PAGE_SIZE];
            System.arraycopy(buffer.array(), 0, copy, 0, Store.PAGE_SIZE);
            pages.put(pageId, copy);
        }

        @Override public void writeBootstrap(long headPacked, long tailPacked) {
            bootstrapHead = headPacked;
            bootstrapTail = tailPacked;
        }

        @Override public long allocateRaw() {
            long id = next.getAndIncrement();
            pages.put(id, new byte[Store.PAGE_SIZE]);
            return id;
        }

        @Override public long currentEpoch() {
            return epoch.get();
        }

        void advanceEpoch() { epoch.incrementAndGet(); }
    }

    private void openInMemory() {
        io = new TestIO();
        long page = io.allocateRaw();
        io.writePage(page, FreeLog.emptyPage());
        bootstrapHead = bootstrapTail = FreeLog.pack(page, 0);
        freeLog = new FreeLog(io, bootstrapHead, bootstrapTail);
    }

    /** Retire + advance the io's current epoch, mirroring PageAllocator.retire's per-call advance. */
    private void retireWithAdvance(long pageId) {
        long e = io.currentEpoch();
        freeLog.retire(pageId, e);
        io.advanceEpoch();
    }

    @Test
    void emptyLogAcquiresNothing() {
        openInMemory();
        assertTrue(freeLog.acquireSafe(100L).isEmpty());
        assertFalse(freeLog.hasSafe(100L));
    }

    @Test
    void headUnsafeReturnsEmpty() {
        openInMemory();
        freeLog.retire(42L, 5L);
        io.advanceEpoch(); // now current > 5
        assertFalse(freeLog.hasSafe(5L)); // 5 < 5 false
        assertTrue(freeLog.acquireSafe(5L).isEmpty());
        // safe once safeEpoch exceeds the retire epoch
        assertEquals(Optional.of(42L), freeLog.acquireSafe(6L));
    }

    @Test
    void oldestSafeFirstFifo() {
        openInMemory();
        freeLog.retire(10L, 1L);
        freeLog.retire(20L, 2L);
        freeLog.retire(30L, 3L);
        // safeEpoch 3 -> only epochs < 3 (1 and 2)
        assertEquals(Optional.of(10L), freeLog.acquireSafe(3L));
        assertEquals(Optional.of(20L), freeLog.acquireSafe(3L));
        assertTrue(freeLog.acquireSafe(3L).isEmpty()); // epoch 3 not < 3
    }

    @Test
    void capacityOverflowGrowsTailPage() {
        openInMemory();
        // Retire more than CAPACITY records — forces a tail-page grow via allocateRaw. After draining,
        // every original id must come back (plus any recycled free-log page ids, which are legitimate
        // free pages too — collected into a set rather than strict FIFO).
        int over = FreeLog.CAPACITY + 50;
        for (int i = 0; i < over; i++) {
            retireWithAdvance(1000L + i);
        }
        long safe = io.currentEpoch() + 1;
        java.util.Set<Long> got = new java.util.HashSet<>();
        Optional<Long> r;
        while ((r = freeLog.acquireSafe(safe)).isPresent()) {
            got.add(r.get());
        }
        for (int i = 0; i < over; i++) {
            assertTrue(got.contains(1000L + i), "missing original id " + i);
        }
    }

    @Test
    void drainRecyclesHeadPageIntoDataPageId() {
        openInMemory();
        long firstTailPage = bootstrapTail >>> 32; // the initial single page
        // Fill exactly one page, then a bit more, so the head page gets a successor and is recycled.
        for (int i = 0; i < FreeLog.CAPACITY + 1; i++) {
            retireWithAdvance(5000L + i);
        }
        // Drain everything; the recycled head-page id must eventually come back as a free id.
        long safe = io.currentEpoch() + 1;
        boolean foundRecycled = false;
        Optional<Long> r;
        while ((r = freeLog.acquireSafe(safe)).isPresent()) {
            if (r.get() == firstTailPage) {
                foundRecycled = true;
            }
        }
        assertTrue(foundRecycled, "drained head page should re-enter the log as a recyclable id");
    }

    @Test
    void singlePageEmptiedDoesNotRecycle() {
        openInMemory();
        long onlyPage = bootstrapHead >>> 32; // the single free-log page before any ops
        freeLog.retire(7L, 1L);
        assertEquals(Optional.of(7L), freeLog.acquireSafe(10L));
        // The single page should still be the head/tail (not recycled, not advanced).
        long headPage = bootstrapHead >>> 32;
        long tailPage = bootstrapTail >>> 32;
        assertEquals(onlyPage, headPage);
        assertEquals(onlyPage, tailPage);
        // And re-usable: a fresh retire should land on it.
        freeLog.retire(8L, 2L);
        assertEquals(Optional.of(8L), freeLog.acquireSafe(10L));
    }

    @Test
    void manyEntriesRoundTripAcrossPages() {
        openInMemory();
        int n = FreeLog.CAPACITY * 3 + 17; // spans ~4 pages
        for (int i = 0; i < n; i++) {
            retireWithAdvance(9000L + i);
        }
        long safe = io.currentEpoch() + 1;
        java.util.Set<Long> got = new java.util.HashSet<>();
        Optional<Long> r;
        while ((r = freeLog.acquireSafe(safe)).isPresent()) {
            got.add(r.get());
        }
        for (int i = 0; i < n; i++) {
            assertTrue(got.contains(9000L + i), "missing original id " + i);
        }
    }

    @Test
    void reopenReadsHeadAndTailFromBootstrap() {
        openInMemory();
        // Retire 3, consume 1 — head advances past record 1. Reopen should resume at record 2.
        retireWithAdvance(1L);
        retireWithAdvance(2L);
        retireWithAdvance(3L);
        freeLog.acquireSafe(Long.MAX_VALUE); // consume record 1

        FreeLog reopened = new FreeLog(io, bootstrapHead, bootstrapTail);
        // Next two acquires are records 2 and 3 (the recycled head-page id, if any, sorts after by
        // epoch — but with < CAPACITY records no recycle happens here).
        assertEquals(Optional.of(2L), reopened.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(3L), reopened.acquireSafe(Long.MAX_VALUE));
    }
}
