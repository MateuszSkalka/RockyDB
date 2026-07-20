package org.rockydb;

public interface PageRef extends AutoCloseable {
    long id();

    byte[] bytes();

    @Override
    void close();
}
