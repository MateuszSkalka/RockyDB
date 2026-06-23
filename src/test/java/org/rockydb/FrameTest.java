package org.rockydb;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class FrameTest {

    @Test
    void newFrameIsFreeWithDefaultIdentity() {
        Frame given = new Frame(0);

        assertTrue(given.isFree());
        assertEquals(Frame.FREE, given.pageId());
    }

    @Test
    void setPageIdUpdatesIdentity() {
        Frame given = new Frame(0);

        given.setPageId(123L);

        assertEquals(123L, given.pageId());
        assertFalse(given.isFree());
    }

    @Test
    void tryClaimSucceedsOnUnpinnedFrame() {
        Frame given = new Frame(0);

        boolean result = given.tryClaim();

        assertTrue(result);
        given.releaseClaim();
    }

    @Test
    void tryClaimFailsOnPinnedFrame() {
        Frame given = new Frame(0);
        given.pin();

        boolean result = given.tryClaim();

        assertFalse(result);
        given.unpin();
    }

    @Test
    void pinAndUnpinAllowsSubsequentClaim() {
        Frame given = new Frame(0);
        given.pin();
        given.unpin();

        boolean result = given.tryClaim();

        assertTrue(result);
        given.releaseClaim();
    }

    @Test
    void unpinWithoutPinThrowsAndResetsCount() {
        Frame given = new Frame(0);

        assertThrows(IllegalStateException.class, given::unpin);

        assertTrue(given.tryClaim());
        given.releaseClaim();
    }

    @Test
    void bumpUsageCapsAtMaxUsage() {
        Frame given = new Frame(0);
        for (int i = 0; i < Frame.MAX_USAGE + 5; i++) {
            given.bumpUsage();
        }

        for (int i = 0; i < Frame.MAX_USAGE; i++) {
            assertTrue(given.hasUsage());
            given.decrementUsage();
        }

        assertFalse(given.hasUsage());
    }

    @Test
    void dirtyFlagToggles() {
        Frame given = new Frame(0);

        given.setDirty(true);
        assertTrue(given.isDirty());

        given.setDirty(false);
        assertFalse(given.isDirty());
    }

    @Test
    void treeLatchExcludesOtherThread() throws Exception {
        Frame given = new Frame(0);
        given.treeLatch().lock();

        AtomicBoolean acquiredByOther = new AtomicBoolean(true);
        Thread other = new Thread(() -> acquiredByOther.set(given.treeLatch().tryLock()));
        other.start();
        other.join();

        assertFalse(acquiredByOther.get());
        given.treeLatch().unlock();
    }
}
