package org.rockydb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A single buffer-pool slot. One frame caches exactly one disk page (identified by
 * {@link #pageId()}). The page bytes are guarded by {@link #ioLock}, which also serializes
 * changes to the frame's identity — a frame is only ever repurposed (its {@code pageId} and
 * bytes replaced) under the write side of {@code ioLock}, and only read accessors that have
 * re-verified {@code pageId} under their lock may rely on the bytes.
 * <p>
 * Concurrency is controlled by two atomic counters:
 * <ul>
 *   <li>{@code pinCount} — a reference count of in-flight users; a frame with {@code pinCount > 0}
 *       cannot be claimed for eviction.</li>
 *   <li>{@code usageCount} — the clock-sweep second-chance counter (0..{@link #MAX_USAGE}),
 *       incremented on each access and decremented on each sweep.</li>
 * </ul>
 */
final class Frame {
    /** Maximum second-chance usage count (mirrors PostgreSQL's BM_MAX_USAGE_COUNT). */
    static final int MAX_USAGE = 5;

    /** Sentinel page id meaning the frame holds no page. */
    static final long FREE = -1L;

    final int index;
    final byte[] bytes = new byte[Store.PAGE_SIZE];
    final ReadWriteLock ioLock = new ReentrantReadWriteLock();

    /**
     * The B-link tree's per-node write latch. Distinct from {@link #ioLock}: {@code ioLock} guards a
     * single page access (tear-free bytes / identity); {@code treeLatch} serializes a whole tree
     * read-modify-write (insert + split propagation) against other writers on the same logical node.
     * Lives on the frame (the cached page), so it is acquired/released via {@link BufferedPool} while
     * the frame is pinned — a pinned frame cannot be evicted, so the latch identity stays stable for
     * the operation's duration.
     */
    private final Lock treeLatch = new ReentrantLock();

    // Identity and dirty flag are volatile: written under ioLock.writeLock(), read either under
    // ioLock or via bare volatile reads (statistics). The atomic counters manage their own memory.
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

    /** The B-link tree write latch for the page currently cached in this frame. */
    Lock treeLatch() {
        return treeLatch;
    }

    boolean isDirty() {
        return dirty;
    }

    void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * Pin the frame for ordinary access. The count stacks, so multiple threads may use the
     * same page concurrently without serializing against each other.
     */
    void pin() {
        pinCount.getAndIncrement();
    }

    /** Release an ordinary pin acquired with {@link #pin()}. */
    void unpin() {
        releasePin();
    }

    /**
     * Exclusively claim the frame for eviction. Succeeds only when no thread is using it
     * ({@code pinCount == 0}). The claim itself is a pin (pinCount becomes 1).
     */
    boolean tryClaim() {
        return pinCount.compareAndSet(0, 1);
    }

    /** Release an eviction claim (after a second-chance pass or when aborting a load). */
    void releaseClaim() {
        releasePin();
    }

    /**
     * Decrement the pin count, failing fast on underflow. A negative {@code pinCount} means a
     * pin was released more often than acquired — a bug that, left unchecked, makes this frame
     * permanently un-evictable ({@link #tryClaim} could never {@code CAS 0 -> 1} again) and
     * eventually starves the pool. Resetting to 0 keeps the frame reclaimable while throwing
     * loudly so the offending caller is pinpointed instead of the pool dying silently later.
     */
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

    /** Decrement the usage count (called by the clock sweep when granting a second chance). */
    void decrementUsage() {
        usageCount.decrementAndGet();
    }

    /** Increment the usage count, capped at {@link #MAX_USAGE}. */
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
