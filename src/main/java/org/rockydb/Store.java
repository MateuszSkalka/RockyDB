package org.rockydb;

public interface Store {
    Node readNode(long id);

    Node writeNode(Node node);

    void updateRootId(long id);

    long rootId();
}
