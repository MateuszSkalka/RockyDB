package org.rockydb;

import java.util.function.Supplier;

public interface Store {
    int PAGE_SIZE = 8 * 1024;
    int PAGE_HEADERS_SIZE = 5;
    int KEY_PREFIX_SIZE = 4;
    int VALUE_POINTER_SIZE = 8;
    int LINK_POINTER_SIZE = VALUE_POINTER_SIZE;

    Node readNode(long id);

    Node writeNode(Node node);

    Supplier<Long> nodeIdGenerator();

    void updateRootId(long id);

    long rootId();
}
