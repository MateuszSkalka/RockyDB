package org.rockydb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class Frame {
    static final int MAX_USAGE = 5;
    static final long FREE = -1L;

    final int index;
    final byte[] bytes = new byte[Store.PAGE_SIZE];
    final ReadWriteLock ioLock = new ReentrantReadWriteLock();
    private final Lock treeLatch = new ReentrantLock();
    private volatile long pageId = FREE;
    private final AtomicInteger pinCount = new AtomicInteger();
    private final AtomicInteger usageCount = new AtomicInteger();
    private volatile boolean dirty = false;

    Frame(int index) {
        this.index = index;
    }

    long pageId() {
        return pageId;
    }

    void setPageId(long pageId) {
        this.pageId = pageId;
    }

    boolean isFree() {
        return pageId == FREE;
    }

    Lock treeLatch() {
        return treeLatch;
    }

    boolean isDirty() {
        return dirty;
    }

    void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    void pin() {
        pinCount.getAndIncrement();
    }

    void unpin() {
        releasePin();
    }

    boolean tryClaim() {
        return pinCount.compareAndSet(0, 1);
    }

    void releaseClaim() {
        releasePin();
    }

    private void releasePin() {
        int after = pinCount.decrementAndGet();
        if (after < 0) {
            pinCount.set(0);
            throw new IllegalStateException("pin count underflow on frame " + index);
        }
    }

    boolean hasUsage() {
        return usageCount.get() > 0;
    }

    void decrementUsage() {
        usageCount.decrementAndGet();
    }

    void bumpUsage() {
        int current;
        do {
            current = usageCount.get();
            if (current >= MAX_USAGE) {
                return;
            }
        } while (!usageCount.compareAndSet(current, current + 1));
    }
}
