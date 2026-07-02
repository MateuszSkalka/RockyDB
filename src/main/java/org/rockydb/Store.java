package org.rockydb;

import java.util.function.Supplier;

public interface Store {
    int PAGE_SIZE = 8 * 1024;
    int PAGE_HEADERS_SIZE = 5;
    int KEY_PREFIX_SIZE = 4;
    int VALUE_POINTER_SIZE = 8;
    int LINK_POINTER_SIZE = VALUE_POINTER_SIZE;

    /**
     * Fraction of {@link Node#MAX_NODE_SIZE} (by bytes) below which a leaf is considered underfilled
     * and a candidate for merging into its right sibling on delete. InnoDB's MERGE_THRESHOLD is 0.50;
     * Lanin–Shasha merge at empty (0.0). 0.30 is a middle ground — aggressive enough to reclaim space,
     * conservative enough to avoid split/merge thrash.
     */
    double MERGE_THRESHOLD = 0.30;

    Node readNode(long id);
    Node writeNode(Node node);
    WriteHandle latchForWrite(long id);
    Supplier<Long> nodeIdGenerator();
    void updateRootId(long id);
    long rootId();
}
