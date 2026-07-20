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
        assertTrue(freeLog.acquireSafe(5L).isEmpty());
        // safe once safeEpoch exceeds the retire epoch
        assertEquals(Optional.of(42L), freeLog.acquireSafe(6L));
    }

    @Test
    void oldestSafeFirstFifo() throws Exception {
        openStore();
        freeLog.retire(10L, 1L);
        freeLog.retire(20L, 2L);
        freeLog.retire(30L, 3L);
        assertEquals(Optional.of(10L), freeLog.acquireSafe(3L));
        assertEquals(Optional.of(20L), freeLog.acquireSafe(3L));
        assertTrue(freeLog.acquireSafe(3L).isEmpty());
    }

    @Test
    void capacityOverflowGrowsTailPage() throws Exception {
        openStore();
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
        long firstTailPage = tailPage();
        for (int i = 0; i < FreeLog.CAPACITY + 1; i++) {
            retireWithAdvance(5000L + i);
        }
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
        long onlyPage = headPage();
        freeLog.retire(7L, 1L);
        assertEquals(Optional.of(7L), freeLog.acquireSafe(10L));
        assertEquals(onlyPage, headPage());
        assertEquals(onlyPage, tailPage());
        freeLog.retire(8L, 2L);
        assertEquals(Optional.of(8L), freeLog.acquireSafe(10L));
    }

    @Test
    void manyEntriesRoundTripAcrossPages() throws Exception {
        openStore();
        int n = FreeLog.CAPACITY * 3 + 17;
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
        freeLog.flush();

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
        freeLog.flush();

        assertEquals(Optional.of(1L), freeLog.acquireSafe(Long.MAX_VALUE));

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
        assertEquals(0, disc.freeLogTailPacked() & 0xFFFFFFFFL);

        retireWithAdvance(1000L);

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

        assertEquals(Optional.of(1L), freeLog.acquireSafe(Long.MAX_VALUE));

        FreeLog afterCrash = new FreeLog(disc, epochs, disc.freeLogHeadPacked(), disc.freeLogTailPacked());
        assertTrue(afterCrash.acquireSafe(Long.MAX_VALUE).isEmpty());
    }

    @Test
    void pendingRetiresAreServedFromWriteBufferWhenDiskIsEmpty() throws Exception {
        openStore();
        retireWithAdvance(7L);
        retireWithAdvance(8L);

        assertEquals(Optional.of(7L), freeLog.acquireSafe(Long.MAX_VALUE));
        assertEquals(Optional.of(8L), freeLog.acquireSafe(Long.MAX_VALUE));
        assertTrue(freeLog.acquireSafe(Long.MAX_VALUE).isEmpty());
    }
}
