package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.*;

public class BLinkTree {
    private final Store store;
    private final RootRef rootRef;

    /**
     * Single-tree constructor: the tree's root is the store's single root id (page 0). Used by the
     * catalog and by any one-tree-at-a-time caller; preserves the pre-multi-tree behavior exactly.
     */
    public BLinkTree(Store store) {
        this(store, new StoreBackedRootRef(store));
    }

    /**
     * Multi-tree constructor: the tree's root is resolved and updated through {@code rootRef},
     * decoupling it from the store's single root id (which then belongs to the catalog tree).
     */
    public BLinkTree(Store store, RootRef rootRef) {
        this.store = store;
        this.rootRef = rootRef;
    }

    public Value get(Value key) {
        long rootId = rootRef.get();
        Node node = store.readNode(rootId);
        while (node.nextNode(key) != -1) {
            node = store.readNode(node.nextNode(key));
        }
        return ((LeafNode) node).getValueForKey(key);
    }

    public void addValue(Value key, Value value) {
        Deque<Long> ancestors = new ArrayDeque<>();
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
            if (!node.isRightLink(currentId)) {
                ancestors.push(node.id());
            }
            node = store.readNode(currentId);
        }

        // Latch the target leaf via a pinned frame handle. All latches are released in the finally,
        // so any exception (BufferExhaustedException, wrapped IOException) cannot leave a latch held.
        WriteHandle handle = store.latchForWrite(currentId);
        try {
            LeafNode leaf = (LeafNode) handle.get();

            // Follow right-links to the correct leaf, lock-coupling: acquire the next latch before
            // releasing the current one. The next handle is tracked in `handle` before it is read,
            // so a read failure is cleaned up by the outer finally.
            while (leaf.nextNode(key) != -1) {
                WriteHandle next = store.latchForWrite(leaf.nextNode(key));
                handle.close();
                handle = next;
                leaf = (LeafNode) handle.get();
            }

            boolean isRoot = rootRef.get() == leaf.id();
            CreationResult result = leaf.copyWith(key, value, store.nodeIdGenerator());

            while (result.promotedValue() != null) {
                // Persist the split: the right sibling first, then the left half (whose link -> right),
                // so a reader following the link always finds the sibling.
                Node rightChild = store.writeNode(result.right());
                handle.set(result.left());
                Node leftChild = result.left();

                if (isRoot) {
                    createNewRoot(leftChild, rightChild, result.promotedValue());
                    result = null;
                    break;
                }

                long parentId;
                if (!ancestors.isEmpty()) {
                    parentId = ancestors.pop();
                } else {
                    // The tree grew deeper during this insert (ancestors exhausted). Re-descend
                    // from the current root to the parent level — deterministic, so there is no
                    // retry and no failure path. The move-right below then locates the exact
                    // parent from any node this lands on at that level.
                    parentId = descendToLevel(result.promotedValue(), leftChild.height() + 1);
                }

                // Latch the parent and propagate. On failure the parent handle is released; on
                // success it transfers to `handle` (held across the next iteration).
                WriteHandle parent = store.latchForWrite(parentId);
                try {
                    BranchNode parentNode = (BranchNode) parent.get();

                    // Move right on the parent level with lock-coupling.
                    while (parentNode.shouldGoRight(result.promotedValue())) {
                        WriteHandle nextParent = store.latchForWrite(parentNode.link());
                        parent.close();
                        parent = nextParent;
                        parentNode = (BranchNode) parent.get();
                    }

                    isRoot = rootRef.get() == parentNode.id();
                    result = parentNode.copyWith(
                            result.promotedValue(), rightChild.id(), rightChild.biggestKey(), store.nodeIdGenerator());
                } catch (RuntimeException e) {
                    parent.close();
                    throw e;
                }
                handle.close();
                handle = parent;
            }

            if (result != null) {
                handle.set(result.left());
            }
        } finally {
            handle.close();
        }
    }

    /**
     * Delete-only removal: physically drops {@code key} from its leaf, with no merge,
     * redistribution, or upward propagation. This keeps the hot-path latch discipline identical to
     * the no-split branch of {@link #addValue} — a single leaf {@link WriteHandle}, released in a
     * {@code finally} — and leaves every B-link invariant intact (links are never removed, so the
     * lock-free {@link #get} is unaffected). An emptied leaf lingers linked and self-heals on the
     * next insert; space reclamation is deliberately deferred.
     */
    public void delete(Value key) {
        // Lock-free descent to the leaf level, like get. No ancestors stack is kept: deletion never
        // propagates upward (no merge, no split), so the parent chain is irrelevant.
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
            node = store.readNode(currentId);
        }

        // Latch the target leaf and follow right-links with lock-coupling (acquire the next latch
        // before releasing the current one), so a concurrent split that moved the key into a right
        // sibling is always caught up. Identical to addValue's preamble.
        WriteHandle handle = store.latchForWrite(currentId);
        try {
            LeafNode leaf = (LeafNode) handle.get();
            while (leaf.nextNode(key) != -1) {
                WriteHandle next = store.latchForWrite(leaf.nextNode(key));
                handle.close();
                handle = next;
                leaf = (LeafNode) handle.get();
            }

            // Physically remove the key in place. `without` returns null when the key is absent,
            // so we skip the write and avoid marking the frame dirty for nothing.
            LeafNode updated = leaf.without(key);
            if (updated != null) {
                handle.set(updated);
            }
        } finally {
            handle.close();
        }
    }

    private void createNewRoot(Node leftChild, Node rightChild, Value promotedValue) {
        Node newRoot = store.writeNode(new BranchNode(
                        store.nodeIdGenerator().get(),
                        leftChild.height() + 1,
                        new Value[]{promotedValue, rightChild.biggestKey()},
                        new long[]{leftChild.id(), rightChild.id()},
                        -1L
                )
        );
        rootRef.set(newRoot.id());
    }

    /**
     * Re-descend from the current root to the first node at {@code targetHeight} on the search
     * path for {@code key}, returning its page id. Used when the {@code ancestors} stack is
     * exhausted — i.e. the tree grew deeper during this insert, so the recorded parent chain no
     * longer reaches the level we need.
     * <p>
     * Lock-free, like {@link #get}: it reads the root id fresh via the tree's {@link RootRef}, so
     * it never observes a stale, too-shallow tree. {@code targetHeight} is always {@code >= 2} (the
     * parent of a split node), so every node visited is a branch and {@code nextNode} never returns
     * {@code -1}; the descent terminates in at most {@code rootHeight - targetHeight} levels. Any
     * node at the target level on the path is a valid starting point, because the caller latches it
     * and move-rights to the exact parent.
     */
    private long descendToLevel(Value key, int targetHeight) {
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (node.height() > targetHeight) {
            currentId = node.nextNode(key);
            node = store.readNode(currentId);
        }
        return currentId;
    }

    /**
     * A {@link RootRef} backed by the store's single root id (page 0). Used by the catalog tree and
     * the single-tree {@link #BLinkTree(Store)} constructor — the case where the tree's root
     * <em>is</em> the store's root.
     */
    private static final class StoreBackedRootRef implements RootRef {
        private final Store store;

        StoreBackedRootRef(Store store) {
            this.store = store;
        }

        @Override
        public long get() {
            return store.rootId();
        }

        @Override
        public void set(long rootId) {
            store.updateRootId(rootId);
        }
    }
}
