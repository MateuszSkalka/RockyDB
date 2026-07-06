package org.rockydb;

import java.util.function.Supplier;

public interface Store {
    int PAGE_SIZE = 8 * 1024;
    int PAGE_HEADERS_SIZE = 5;
    int KEY_PREFIX_SIZE = 4;
    int VALUE_POINTER_SIZE = 8;
    int LINK_POINTER_SIZE = VALUE_POINTER_SIZE;
    double MERGE_THRESHOLD = 0.30;

    Node readNode(long id);
    Node writeNode(Node node);
    WriteHandle latchForWrite(long id);
    void updateRootId(long id);
    long rootId();

    PageAllocator allocator();

    long enterOp();

    void exitOp(long ticket);

    void retire(long pageId);
}
