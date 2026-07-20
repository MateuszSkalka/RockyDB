package org.rockydb;

public interface RootRef {
    long get();
    void set(long rootId);
    long count();
    void addToCount(long delta);
}
