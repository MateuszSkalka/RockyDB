package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.*;

public class BLinkTree {
    private final Store store;

    public BLinkTree(Store store) {
        this.store = store;
    }

    public Value get(Value key) {
        long rootId = store.rootId();
        Node node = store.readNode(rootId);
        while (node.nextNode(key) != -1) {
            node = store.readNode(node.nextNode(key));
        }
        return ((LeafNode) node).getValueForKey(key);
    }

    public void addValue(Value key, Value value) {
        Deque<Long> ancestors = new ArrayDeque<>();
        long currentId = store.rootId();
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

            boolean isRoot = store.rootId() == leaf.id();
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

                    isRoot = store.rootId() == parentNode.id();
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

    private void createNewRoot(Node leftChild, Node rightChild, Value promotedValue) {
        Node newRoot = store.writeNode(new BranchNode(
                        store.nodeIdGenerator().get(),
                        leftChild.height() + 1,
                        new Value[]{promotedValue, rightChild.biggestKey()},
                        new long[]{leftChild.id(), rightChild.id()},
                        -1L
                )
        );
        store.updateRootId(newRoot.id());
    }

    /**
     * Re-descend from the current root to the first node at {@code targetHeight} on the search
     * path for {@code key}, returning its page id. Used when the {@code ancestors} stack is
     * exhausted — i.e. the tree grew deeper during this insert, so the recorded parent chain no
     * longer reaches the level we need.
     * <p>
     * Lock-free, like {@link #get}: it reads {@link Store#rootId()} fresh, so it never observes a
     * stale, too-shallow tree. {@code targetHeight} is always {@code >= 2} (the parent of a split
     * node), so every node visited is a branch and {@code nextNode} never returns {@code -1}; the
     * descent terminates in at most {@code rootHeight - targetHeight} levels. Any node at the
     * target level on the path is a valid starting point, because the caller latches it and
     * move-rights to the exact parent.
     */
    private long descendToLevel(Value key, int targetHeight) {
        long currentId = store.rootId();
        Node node = store.readNode(currentId);
        while (node.height() > targetHeight) {
            currentId = node.nextNode(key);
            node = store.readNode(currentId);
        }
        return currentId;
    }
}
