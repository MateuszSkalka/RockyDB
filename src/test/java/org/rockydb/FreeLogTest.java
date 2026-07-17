package org.rockydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FreeLogTest {

    private File dbFile;
    private DiscStore disc;
    private EpochManager epochs;
    private FreeLog freeLog;

    private void openStore() throws IOException {
        dbFile = File.createTempFile("rockydb-freelog-", ".db");
        dbFile.deleteOnExit();
        disc = new DiscStore(dbFile);
        epochs = new EpochManager(0);
        long page = disc.allocateRaw();
        disc.writeRawPage(page, FreeLog.emptyPage());
        long packed = FreeLog.pack(page, 0);
        disc.writeFreeLogBootstrap(packed, packed);
        freeLog = new FreeLog(disc, epochs, packed, packed);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (disc != null) {
            disc.close();
            disc = null;
        }
        if (dbFile != null && dbFile.exists()) {
            dbFile.delete();
        }
    }

    /** Retire + advance the current epoch, mirroring PageAllocator.retire's per-call advance. */
    private void retireWithAdvance(long pageId) {
        freeLog.retire(pageId, epochs.current());
        epochs.advance();
    }

    private long headPage() {
        return disc.freeLogHeadPacked() >>> 32;
    }

    private long tailPage() {
        return disc.freeLogTailPacked() >>> 32;
    }

    @Test
    void emptyLogAcquiresNothing() throws Exception {
        openStore();
        assertTrue(freeLog.acquireSafe(100L).isEmpty());
    }

    @Test
    void headUnsafeReturnsEmpty() throws Exception {
        openStore();
        freeLog.retire(42L, 5L);
        assertTrue(freeLog.acquireSafe(5L).isEmpty()); // 5 < 5 false
        // safe once safeEpoch exceeds the retire epoch
        assertEquals(Optional.of(42L), freeLog.acquireSafe(6L));
    }

    @Test
    void oldestSafeFirstFifo() throws Exception {
        openStore();
        freeLog.retire(10L, 1L);
        freeLog.retire(20L, 2L);
        freeLog.retire(30L, 3L);
        // safeEpoch 3 -> only epochs < 3 (1 and 2)
        assertEquals(Optional.of(10L), freeLog.acquireSafe(3L));
        assertEquals(Optional.of(20L), freeLog.acquireSafe(3L));
        assertTrue(freeLog.acquireSafe(3L).isEmpty()); // epoch 3 not < 3
    }

    @Test
    void capacityOverflowGrowsTailPage() throws Exception {
        openStore();
        // Retire more than CAPACITY records — forces a tail-page grow via allocateRaw. After draining,
        // every original id must come back (plus any recycled free-log page ids, which are legitimate
        // free pages too — collected into a set rather than strict FIFO).
        int over = FreeLog.CAPACITY + 50;
        for (int i = 0; i < over; i++) {
            retireWithAdvance(1000L + i);
        }
        long safe = epochs.current() + 1;
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
    void drainRecyclesHeadPageIntoDataPageId() throws Exception {
        openStore();
        long firstTailPage = tailPage(); // the initial single page
        // Fill exactly one page, then a bit more, so the head page gets a successor and is recycled.
        for (int i = 0; i < FreeLog.CAPACITY + 1; i++) {
            retireWithAdvance(5000L + i);
        }
        // Drain everything; the recycled head-page id must eventually come back as a free id.
        long safe = epochs.current() + 1;
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
    void singlePageEmptiedDoesNotRecycle() throws Exception {
        openStore();
        long onlyPage = headPage(); // the single free-log page before any ops
        freeLog.retire(7L, 1L);
        assertEquals(Optional.of(7L), freeLog.acquireSafe(10L));
        // The single page should still be the head/tail (not recycled, not advanced).
        assertEquals(onlyPage, headPage());
        assertEquals(onlyPage, tailPage());
        // And re-usable: a fresh retire should land on it.
        freeLog.retire(8L, 2L);
        assertEquals(Optional.of(8L), freeLog.acquireSafe(10L));
    }

    @Test
    void manyEntriesRoundTripAcrossPages() throws Exception {
        openStore();
        int n = FreeLog.CAPACITY * 3 + 17; // spans ~4 pages
        for (int i = 0; i < n; i++) {
            retireWithAdvance(9000L + i);
        }
        long safe = epochs.current() + 1;
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
    void reopenReadsHeadAndTailFromBootstrap() throws Exception {
        openStore();
        retireWithAdvance(1L);
        retireWithAdvance(2L);
        retireWithAdvance(3L);
        freeLog.flush(); // retire is buffered; persist before reopen

        FreeLog reopened = new FreeLog(disc, epochs, disc.freeLogHeadPacked(), disc.freeLogTailPacked());
        assertEquals(Optional.of(1L), reopened.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(2L), reopened.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(3L), reopened.acquireSafe(Long.MAX_VALUE));
    }

    @Test
    void midPageHeadPositionSurvivesReopen() throws Exception {
        openStore();
        for (int i = 0; i < FreeLog.BUFFER_CAPACITY + 10; i++) {
            retireWithAdvance(1L + i);
        }
        freeLog.flush(); // 266 records on disk: a full 256-record batch + 10 more

        // first refill consumes the 256-record batch on disk; the unserved ones stay in memory
        assertEquals(Optional.of(1L), freeLog.acquireSafe(Long.MAX_VALUE));

        // reopen: the head was persisted mid-page past the consumed batch; the 255 unserved
        // entries leak (accepted), and the 10 records still on disk resume correctly
        FreeLog reopened = new FreeLog(disc, epochs, disc.freeLogHeadPacked(), disc.freeLogTailPacked());
        for (int i = 0; i < 10; i++) {
            assertEquals(Optional.of(257L + i), reopened.acquireSafe(Long.MAX_VALUE));
        }
        assertTrue(reopened.acquireSafe(Long.MAX_VALUE).isEmpty());
    }

    @Test
    void retiresAreBufferedUntilCapacityThenFlush() throws Exception {
        openStore();
        for (int i = 0; i < FreeLog.BUFFER_CAPACITY - 1; i++) {
            retireWithAdvance(100L + i);
        }
        // nothing persisted yet — tail still shows zero records
        assertEquals(0, disc.freeLogTailPacked() & 0xFFFFFFFFL);

        retireWithAdvance(1000L); // 256th entry fills the buffer and triggers the flush

        assertEquals(FreeLog.BUFFER_CAPACITY, disc.freeLogTailPacked() & 0xFFFFFFFFL);
    }

    @Test
    void flushPersistsPendingRetires() throws Exception {
        openStore();
        retireWithAdvance(1L);
        retireWithAdvance(2L);
        freeLog.flush();

        FreeLog reopened = new FreeLog(disc, epochs, disc.freeLogHeadPacked(), disc.freeLogTailPacked());
        assertEquals(Optional.of(1L), reopened.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(2L), reopened.acquireSafe(Long.MAX_VALUE));
    }

    @Test
    void refillConsumesEntriesOnDiskSoCrashCannotResurrectServedPages() throws Exception {
        openStore();
        retireWithAdvance(1L);
        retireWithAdvance(2L);
        retireWithAdvance(3L);
        freeLog.flush();

        // the refill batch-consumes all 3 records on disk, then serves the first from memory
        assertEquals(Optional.of(1L), freeLog.acquireSafe(Long.MAX_VALUE));

        // A "crash" here (reopen without close) loses the 2 unserved read-buffer entries —
        // they leak — but the page already handed out can never be handed out twice.
        FreeLog afterCrash = new FreeLog(disc, epochs, disc.freeLogHeadPacked(), disc.freeLogTailPacked());
        assertTrue(afterCrash.acquireSafe(Long.MAX_VALUE).isEmpty());
    }

    @Test
    void pendingRetiresAreServedFromWriteBufferWhenDiskIsEmpty() throws Exception {
        openStore();
        retireWithAdvance(7L);
        retireWithAdvance(8L);

        // never flushed to disk, still served in FIFO order
        assertEquals(Optional.of(7L), freeLog.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(8L), freeLog.acquireSafe(Long.MAX_VALUE));
        assertTrue(freeLog.acquireSafe(Long.MAX_VALUE).isEmpty());
    }
}
