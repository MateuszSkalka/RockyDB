package org.rockydb;


public interface RootRef {
    long get();
    void set(long rootId);

    /** Exact number of key/value pairs currently in the tree (cached, O(1)). */
    long count();

    /** Adjusts the cached element count; callers pass +1/-1 under the leaf latch. */
    void addToCount(long delta);
}
