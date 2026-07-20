package org.rockydb;

public interface Store {
    int PAGE_SIZE = 8 * 1024;
    double MERGE_THRESHOLD = 0.30;

    PageRef readPage(long id);

    // bumpUsage == false keeps sequential scans from evicting the hot working set
    PageRef readPage(long id, boolean bumpUsage);

    PageWrite latchForWrite(long id);

    PageWrite latchForWriteBlank(long id);

    void updateRootId(long id);

    long rootId();

    long elementCount();

    void addToElementCount(long delta);

    long allocate();

    void retire(long pageId);

    long enterOp();

    void exitOp(long ticket);
}
