package org.rockydb;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

final class FreeLog {
    static final int HEADER_BYTES = Long.BYTES + Integer.BYTES;
    static final int RECORD_BYTES = Long.BYTES + Long.BYTES;
    static final int CAPACITY = (Store.PAGE_SIZE - HEADER_BYTES) / RECORD_BYTES;

    public interface FreeLogIO {
        ByteBuffer readPage(long pageId);
        void writePage(long pageId, ByteBuffer buffer);
        void writeBootstrap(long headPacked, long tailPacked);
        long allocateRaw();
        long currentEpoch();
    }

    private final FreeLogIO io;
    private final ReentrantLock lock = new ReentrantLock();

    private long headPageId;
    private int headSlot;
    private long tailPageId;
    private int tailCount;

    FreeLog(FreeLogIO io, long headPacked, long tailPacked) {
        this.io = io;
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
            ByteBuffer page = io.readPage(tailPageId);
            page.position(HEADER_BYTES + tailCount * RECORD_BYTES);
            page.putLong(pageId);
            page.putLong(freeEpoch);
            tailCount++;
            page.putLong(0, page.getLong(0));
            page.putInt(Long.BYTES, tailCount);
            io.writePage(tailPageId, page);
            io.writeBootstrap(headPacked(), tailPacked());
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
        ByteBuffer headPage = io.readPage(headPageId);
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
            appendToTailNoLock(headPageId, io.currentEpoch());
            headPageId = nextPgId;
            headSlot = 0;
        } else if (singlePageEmptied) {
            headSlot = 0;
            tailCount = 0;
            ByteBuffer p = io.readPage(tailPageId);
            p.putInt(Long.BYTES, 0);
            p.putLong(0, -1L);
            io.writePage(tailPageId, p);
        }
        io.writeBootstrap(headPacked(), tailPacked());
        return pageId;
    }

    private void appendToTailNoLock(long pageId, long freeEpoch) {
        if (tailCount == CAPACITY) {
            growTail();
        }
        ByteBuffer page = io.readPage(tailPageId);
        page.position(HEADER_BYTES + tailCount * RECORD_BYTES);
        page.putLong(pageId);
        page.putLong(freeEpoch);
        tailCount++;
        page.putInt(Long.BYTES, tailCount);
        io.writePage(tailPageId, page);
        io.writeBootstrap(headPacked(), tailPacked());
    }

    private void growTail() {
        long newTail = io.allocateRaw();
        ByteBuffer oldTail = io.readPage(tailPageId);
        oldTail.putLong(0, newTail);
        io.writePage(tailPageId, oldTail);

        ByteBuffer fresh = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        fresh.putLong(0, -1L);       // next = none
        fresh.putInt(Long.BYTES, 0); // count = 0
        io.writePage(newTail, fresh);

        tailPageId = newTail;
        tailCount = 0;
        io.writeBootstrap(headPacked(), tailPacked());
    }

    private long readHeadEpoch() {
        ByteBuffer headPage = io.readPage(headPageId);
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
