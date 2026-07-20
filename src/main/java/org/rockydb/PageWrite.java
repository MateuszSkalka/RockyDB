package org.rockydb;

public interface PageWrite extends AutoCloseable {
    long id();

    byte[] bytes();

    // Publishes the shadow immediately: a merge tombstone on another page may already
    // route readers here, so the mutation must be visible before close().
    void markDirty();

    @Override
    void close();
}
