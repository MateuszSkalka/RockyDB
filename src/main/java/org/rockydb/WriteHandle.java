package org.rockydb;


public interface WriteHandle extends AutoCloseable {

    Node get();

    void set(Node node);

    @Override
    void close();
}
