package org.rockydb;

public interface Store {
    int PAGE_SIZE = 8 * 1024;
    int PAGE_HEADERS_SIZE = 5;
    int KEY_PREFIX_SIZE = 4;
    int VALUE_POINTER_SIZE = 8;

    Node readNode(long id);

    Node writeNode(Node node);

    void updateRootId(long id);

    long rootId();
}
