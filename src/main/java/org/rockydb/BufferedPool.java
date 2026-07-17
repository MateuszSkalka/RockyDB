package org.rockydb;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Raw page cache over a {@link DiscStore}: a fixed set of frames, clock-sweep eviction
 * and pin-based liveness. Deals in bytes only — it knows nothing about nodes or
 * serialization. Callers get a pinned {@link Frame} from {@link #acquire(long)} and
 * must {@link Frame#unpin()} it when done. Dirty pages are written back on eviction
 * and on {@link #close()}.
 */
public class BufferedPool implements Closeable {

    private final DiscStore discStore;
    private final Frame[] frames;
    private final Clock clock;
    private final ConcurrentMap<Long, Frame> pageToFrame = new ConcurrentHashMap<>();

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();

    private volatile boolean closed = false;

    public BufferedPool(DiscStore discStore, int numFrames) {
        if (numFrames <= 0) {
            throw new IllegalArgumentException("numFrames must be > 0");
        }
        this.discStore = discStore;
        this.frames = new Frame[numFrames];
        for (int i = 0; i < numFrames; i++) {
            this.frames[i] = new Frame(i);
        }
        this.clock = new Clock(this.frames);
    }

    /**
     * Returns a pinned frame holding page {@code id}, loading it from disk if necessary.
     * The frame is guaranteed to keep holding that page until unpinned (a pinned frame
     * can never be repurposed by the clock). Content access still needs the frame's
     * {@code ioLock}.
     */
    public Frame acquire(long id) {
        ensureOpen();
        while (true) {
            Frame cached = pageToFrame.get(id);
            if (cached != null) {
                cached.pin();
                cached.ioLock.readLock().lock();
                try {
                    if (cached.pageId() == id) {
                        hits.increment();
                        return cached;
                    }
                } finally {
                    cached.ioLock.readLock().unlock();
                }
                cached.unpin(); // repurposed between lookup and pin; retry
                continue;
            }
            Frame victim = clock.findVictim(); // exclusively claimed (pinCount == 1)
            if (installAndLoad(victim, id)) {
                return victim; // pinned, pageId == id, content loaded
            }
            victim.unpin(); // lost the race for id; release the claim and retry
        }
    }

    private boolean installAndLoad(Frame victim, long id) {
        victim.ioLock.writeLock().lock();
        try {
            Frame existing = pageToFrame.putIfAbsent(id, victim);
            if (existing != null) {
                return false;
            }
            long oldId = victim.pageId();
            if (oldId != Frame.FREE) {

                if (victim.isDirty()) {
                    discStore.writeRawPage(oldId, ByteBuffer.wrap(victim.bytes));
                }
                pageToFrame.remove(oldId, victim);
                evictions.increment();
            }
            ByteBuffer page = discStore.readRawPage(id);
            System.arraycopy(page.array(), 0, victim.bytes, 0, Store.PAGE_SIZE);
            victim.setDirty(false);
            victim.setPageId(id);
            misses.increment();
            return true;
        } finally {
            victim.ioLock.writeLock().unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("BufferedPool is closed");
        }
    }

    /** Flushes all dirty frames to disk. Does not close the underlying {@link DiscStore}. */
    @Override
    public void close() throws IOException {
        closed = true;
        for (Frame frame : frames) {
            frame.ioLock.writeLock().lock();
            try {
                if (!frame.isFree() && frame.isDirty()) {
                    discStore.writeRawPage(frame.pageId(), ByteBuffer.wrap(frame.bytes));
                    frame.setDirty(false);
                }
            } finally {
                frame.ioLock.writeLock().unlock();
            }
        }
    }

    public int getBufferHits() {
        return (int) hits.sum();
    }

    public int getBufferMisses() {
        return (int) misses.sum();
    }

    public int getEvictions() {
        return (int) evictions.sum();
    }

    public int getNumFrames() {
        return frames.length;
    }

    public int getUsedFrames() {
        int count = 0;
        for (Frame frame : frames) {
            if (!frame.isFree()) {
                count++;
            }
        }
        return count;
    }

    public int getDirtyFrames() {
        int count = 0;
        for (Frame frame : frames) {
            if (!frame.isFree() && frame.isDirty()) {
                count++;
            }
        }
        return count;
    }

    public double getHitRatio() {
        long h = hits.sum();
        long m = misses.sum();
        long total = h + m;
        return total == 0 ? 0.0 : (double) h / (double) total;
    }
}
