package org.rockydb;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FIFO queue of retired pages, buffered in memory to amortize disk IO.
 *
 * Disk layout is a chain of raw pages (per-page header: next ptr + count; (pageId, epoch)
 * records), head/tail packed as (pageId << 32) | slot in the file header. On top of that:
 *  - {@link #retire} appends to a write buffer, flushed to the tail of the chain only when
 *    full ({@link #BUFFER_CAPACITY} entries) or on {@link #flush()}.
 *  - {@link #acquireSafe} serves from a read buffer; when empty it refills by consuming up
 *    to BUFFER_CAPACITY records from the head of the chain — persisting the advanced head
 *    immediately, so an already-served page can never be resurrected and handed out twice
 *    after a crash — or, when the on-disk queue is empty, by transferring the write buffer.
 *
 * A page becomes re-allocatable only once its retire epoch is older than the safe epoch
 * (see {@link EpochManager}). Records are served FIFO and epochs are non-decreasing along
 * the queue, so checking the read buffer's head is enough.
 *
 * Crash semantics: unflushed retires and unserved read-buffer entries are lost — a bounded
 * free-space leak (≤ 2 × BUFFER_CAPACITY pages), never corruption. Accepted: there is no WAL.
 *
 * Concurrency: one lock; hot paths are plain memory ops, disk IO happens once per batch.
 */
final class FreeLog {
    static final int HEADER_BYTES = Long.BYTES + Integer.BYTES;
    static final int RECORD_BYTES = Long.BYTES + Long.BYTES;
    static final int CAPACITY = (Store.PAGE_SIZE - HEADER_BYTES) / RECORD_BYTES;
    static final int BUFFER_CAPACITY = 256;

    private record FreePage(long pageId, long epoch) {}

    private final DiscStore disc;
    private final EpochManager epochs;
    private final ReentrantLock lock = new ReentrantLock();

    private final ArrayDeque<FreePage> writeBuffer = new ArrayDeque<>();
    private final ArrayDeque<FreePage> readBuffer = new ArrayDeque<>();

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
            appendToWriteBuffer(new FreePage(pageId, freeEpoch));
        } finally {
            lock.unlock();
        }
    }

    Optional<Long> acquireSafe(long safeEpoch) {
        lock.lock();
        try {
            FreePage head = readBuffer.peek();
            if (head == null) {
                refillReadBuffer();
                head = readBuffer.peek();
            }
            if (head == null || head.epoch() >= safeEpoch) {
                return Optional.empty();
            }
            readBuffer.poll();
            return Optional.of(head.pageId());
        } finally {
            lock.unlock();
        }
    }

    /** Persists pending retires. Called on shutdown; entries already consumed into the read
     *  buffer but not yet served are dropped (bounded, accepted leak — see class doc). */
    void flush() {
        lock.lock();
        try {
            flushWriteBuffer();
        } finally {
            lock.unlock();
        }
    }

    private void appendToWriteBuffer(FreePage fp) {
        writeBuffer.add(fp);
        if (writeBuffer.size() >= BUFFER_CAPACITY) {
            flushWriteBuffer();
        }
    }

    private void flushWriteBuffer() {
        if (writeBuffer.isEmpty()) {
            return;
        }
        ByteBuffer tail = null;
        while (!writeBuffer.isEmpty()) {
            if (tailCount == CAPACITY) {
                if (tail != null) {
                    disc.writeRawPage(tailPageId, tail);
                    tail = null;
                }
                growTail();
            }
            if (tail == null) {
                tail = disc.readRawPage(tailPageId);
            }
            FreePage fp = writeBuffer.poll();
            tail.position(HEADER_BYTES + tailCount * RECORD_BYTES);
            tail.putLong(fp.pageId());
            tail.putLong(fp.epoch());
            tailCount++;
            tail.putInt(Long.BYTES, tailCount); // count lives in the page header
        }
        if (tail != null) {
            disc.writeRawPage(tailPageId, tail);
        }
        // bootstrap last: a crash before this write leaves the whole batch invisible → leak only
        disc.writeFreeLogBootstrap(headPacked(), tailPacked());
    }

    private void refillReadBuffer() {
        boolean consumed = false;
        while (readBuffer.size() < BUFFER_CAPACITY && !queueEmpty()) {
            ByteBuffer headPage = disc.readRawPage(headPageId);
            long next = headPage.getLong(0);
            int count = headPage.getInt(Long.BYTES);
            while (headSlot < count && readBuffer.size() < BUFFER_CAPACITY) {
                headPage.position(HEADER_BYTES + headSlot * RECORD_BYTES);
                readBuffer.add(new FreePage(headPage.getLong(), headPage.getLong()));
                headSlot++;
            }
            consumed = true;
            if (headSlot < count) {
                break; // read buffer filled mid-page; resume here on the next refill
            }
            if (next != -1) {
                // the drained queue page re-enters the log as a fresh free page
                appendToWriteBuffer(new FreePage(headPageId, epochs.current()));
                headPageId = next;
                headSlot = 0;
            } else {
                // last page emptied: reset the queue in place
                headSlot = 0;
                tailCount = 0;
                headPage.putInt(Long.BYTES, 0);
                headPage.putLong(0, -1L);
                disc.writeRawPage(tailPageId, headPage);
            }
        }
        if (consumed) {
            // persist the advanced head NOW: everything in the read buffer is consumed as far
            // as disk is concerned, so a crash can leak but never double-allocate these pages
            disc.writeFreeLogBootstrap(headPacked(), tailPacked());
        }
        if (readBuffer.isEmpty() && queueEmpty()) {
            // nothing persisted: serve directly from pending retires (order is preserved —
            // write buffer entries are newer than anything already consumed)
            readBuffer.addAll(writeBuffer);
            writeBuffer.clear();
        }
    }

    private boolean queueEmpty() {
        return headPageId == tailPageId && headSlot >= tailCount;
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

    static ByteBuffer emptyPage() {
        ByteBuffer b = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        b.putLong(0, -1L);       // next = none
        b.putInt(Long.BYTES, 0); // count = 0
        return b;
    }
}
