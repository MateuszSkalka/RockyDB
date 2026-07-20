package org.rockydb;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

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
                cached.unpin();
                continue;
            }
            Frame victim = clock.findVictim();
            if (installAndLoad(victim, id)) {
                return victim;
            }
            victim.unpin();
        }
    }

    public Frame acquireBlank(long id) {
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
                cached.unpin();
                continue;
            }
            Frame victim = clock.findVictim();
            if (installBlank(victim, id)) {
                return victim;
            }
            victim.unpin();
            continue;
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
            discStore.readRawPage(id, victim.bytes);
            victim.setDirty(false);
            victim.setPageId(id);
            misses.increment();
            return true;
        } finally {
            victim.ioLock.writeLock().unlock();
        }
    }

    private boolean installBlank(Frame victim, long id) {
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
