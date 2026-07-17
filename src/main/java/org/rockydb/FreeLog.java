package org.rockydb;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FIFO queue of retired pages persisted as a chain of raw disk pages. A page becomes
 * re-allocatable only once its retire epoch is older than the safe epoch (see
 * {@link EpochManager}), i.e. no in-flight operation can still reference it.
 * Bypasses the buffer pool deliberately: IO is direct and immediate, and the log
 * never competes with tree pages for frames.
 */
public final class FreeLog {
    static final int HEADER_BYTES = Long.BYTES + Integer.BYTES;
    static final int RECORD_BYTES = Long.BYTES + Long.BYTES;
    static final int CAPACITY = (Store.PAGE_SIZE - HEADER_BYTES) / RECORD_BYTES;

    private final DiscStore disc;
    private final EpochManager epochs;
    private final ReentrantLock lock = new ReentrantLock();

    private long headPageId;
    private int headSlot;
    private long tailPageId;
    private int tailCount;

    FreeLog(DiscStore disc, EpochManager epochs, long headPacked, long tailPacked) {
        this.disc = disc;
        this.epochs = epochs;
        this.headPageId = pageOf(headPacked);
        this.headSlot = slotOf(headPacked);
        this.tailPageId = pageOf(tailPacked);
        this.tailCount = slotOf(tailPacked);
    }

    static long pack(long pageId, int slot) {
        return (pageId << 32) | (slot & 0xFFFFFFFFL);
    }

    private static long pageOf(long packed) {
        return packed >>> 32;
    }

    private static int slotOf(long packed) {
        return (int) (packed & 0xFFFFFFFFL);
    }

    private long headPacked() { return pack(headPageId, headSlot); }
    private long tailPacked() { return pack(tailPageId, tailCount); }

    void retire(long pageId, long freeEpoch) {
        lock.lock();
        try {
            if (tailCount == CAPACITY) {
                growTail();
            }
            ByteBuffer page = disc.readRawPage(tailPageId);
            page.position(HEADER_BYTES + tailCount * RECORD_BYTES);
            page.putLong(pageId);
            page.putLong(freeEpoch);
            tailCount++;
            page.putLong(0, page.getLong(0));
            page.putInt(Long.BYTES, tailCount);
            disc.writeRawPage(tailPageId, page);
            disc.writeFreeLogBootstrap(headPacked(), tailPacked());
        } finally {
            lock.unlock();
        }
    }

    Optional<Long> acquireSafe(long safeEpoch) {
        lock.lock();
        try {
            if (!hasSafeLocked(safeEpoch)) {
                return Optional.empty();
            }
            long result = consumeHead(safeEpoch);
            return result == -1 ? Optional.empty() : Optional.of(result);
        } finally {
            lock.unlock();
        }
    }

    boolean hasSafe(long safeEpoch) {
        lock.lock();
        try {
            return hasSafeLocked(safeEpoch);
        } finally {
            lock.unlock();
        }
    }

    private boolean hasSafeLocked(long safeEpoch) {
        if (headPageId == tailPageId && headSlot >= tailCount) {
            return false;
        }

        long epoch = readHeadEpoch();
        return epoch < safeEpoch;
    }
    private long consumeHead(long safeEpoch) {
        ByteBuffer headPage = disc.readRawPage(headPageId);
        long nextPgId = headPage.getLong(0);
        int count = headPage.getInt(Long.BYTES);
        if (headSlot >= count) {
            return -1;
        }
        headPage.position(HEADER_BYTES + headSlot * RECORD_BYTES);
        long pageId = headPage.getLong();
        long epoch = headPage.getLong();
        if (epoch >= safeEpoch) {
            return -1;
        }
        headSlot++;

        boolean headDrainedWithSuccessor = (headSlot >= count) && (nextPgId != -1);
        boolean singlePageEmptied = (headPageId == tailPageId) && headSlot >= count;
        if (headDrainedWithSuccessor && !singlePageEmptied) {
            appendToTailNoLock(headPageId, epochs.current());
            headPageId = nextPgId;
            headSlot = 0;
        } else if (singlePageEmptied) {
            headSlot = 0;
            tailCount = 0;
            ByteBuffer p = disc.readRawPage(tailPageId);
            p.putInt(Long.BYTES, 0);
            p.putLong(0, -1L);
            disc.writeRawPage(tailPageId, p);
        }
        disc.writeFreeLogBootstrap(headPacked(), tailPacked());
        return pageId;
    }

    private void appendToTailNoLock(long pageId, long freeEpoch) {
        if (tailCount == CAPACITY) {
            growTail();
        }
        ByteBuffer page = disc.readRawPage(tailPageId);
        page.position(HEADER_BYTES + tailCount * RECORD_BYTES);
        page.putLong(pageId);
        page.putLong(freeEpoch);
        tailCount++;
        page.putInt(Long.BYTES, tailCount);
        disc.writeRawPage(tailPageId, page);
        disc.writeFreeLogBootstrap(headPacked(), tailPacked());
    }

    private void growTail() {
        long newTail = disc.allocateRaw();
        ByteBuffer oldTail = disc.readRawPage(tailPageId);
        oldTail.putLong(0, newTail);
        disc.writeRawPage(tailPageId, oldTail);

        disc.writeRawPage(newTail, emptyPage());

        tailPageId = newTail;
        tailCount = 0;
        disc.writeFreeLogBootstrap(headPacked(), tailPacked());
    }

    private long readHeadEpoch() {
        ByteBuffer headPage = disc.readRawPage(headPageId);
        int count = headPage.getInt(Long.BYTES);
        if (headSlot >= count) {
            return Long.MAX_VALUE;
        }
        headPage.position(HEADER_BYTES + headSlot * RECORD_BYTES + Long.BYTES);
        return headPage.getLong();
    }

    static ByteBuffer emptyPage() {
        ByteBuffer b = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        b.putLong(0, -1L);       // next = none
        b.putInt(Long.BYTES, 0); // count = 0
        return b;
    }
}
