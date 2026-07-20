package org.rockydb;

public interface Store {
    int PAGE_SIZE = 8 * 1024;
    double MERGE_THRESHOLD = 0.30;

    /** Pinned + read-locked view of a page; close promptly. Bumps clock usage. */
    PageRef readPage(long id);

    /**
     * Pinned + read-locked view of a page; close promptly.
     * {@code bumpUsage == false} keeps sequential scans from inflating clock usage —
     * scan pages become the preferred eviction victims instead of the hot set.
     */
    PageRef readPage(long id, boolean bumpUsage);

    /** Pinned + tree-latched page; mutations go to a shadow copy published under the
     *  frame's write lock ({@link PageWrite}). */
    PageWrite latchForWrite(long id);

    /**
     * Like {@link #latchForWrite} for a freshly allocated page: skips the disk read
     * of the stale free-slot content — the caller formats the whole page before use.
     */
    PageWrite latchForWriteBlank(long id);

    void updateRootId(long id);

    long rootId();

    /** Element count of the standalone (header-rooted) tree. Kept in memory,
     *  persisted to the file header on close — no per-op I/O. */
    long elementCount();

    void addToElementCount(long delta);

    long allocate();

    void retire(long pageId);

    long enterOp();

    void exitOp(long ticket);
}
