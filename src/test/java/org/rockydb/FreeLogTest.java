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
        // Retire 3, consume 1 — head advances past record 1. Reopen should resume at record 2.
        retireWithAdvance(1L);
        retireWithAdvance(2L);
        retireWithAdvance(3L);
        freeLog.acquireSafe(Long.MAX_VALUE); // consume record 1

        FreeLog reopened = new FreeLog(disc, epochs, disc.freeLogHeadPacked(), disc.freeLogTailPacked());
        // Next two acquires are records 2 and 3 (the recycled head-page id, if any, sorts after by
        // epoch — but with < CAPACITY records no recycle happens here).
        assertEquals(Optional.of(2L), reopened.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(3L), reopened.acquireSafe(Long.MAX_VALUE));
    }
}
