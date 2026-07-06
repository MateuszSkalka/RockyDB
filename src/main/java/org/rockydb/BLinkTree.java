package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.function.Supplier;

public class BLinkTree {
    private final Store store;
    private final RootRef rootRef;

    public BLinkTree(Store store) {
        this(store, new StoreBackedRootRef(store));
    }

    BLinkTree(Store store, RootRef rootRef) {
        this.store = store;
        this.rootRef = rootRef;
    }

    private Supplier<Long> idSupplier() {
        return store.allocator().idSupplier();
    }

    public Value get(Value key) {
        long ticket = store.enterOp();
        try {
            return getUnderEpoch(key);
        } finally {
            store.exitOp(ticket);
        }
    }

    private Value getUnderEpoch(Value key) {
        long rootId = rootRef.get();
        Node node = store.readNode(rootId);
        while (node.nextNode(key) != -1) {
            node = store.readNode(node.nextNode(key));
        }
        return ((LeafNode) node).getValueForKey(key);
    }

    public void addValue(Value key, Value value) {
        long ticket = store.enterOp();
        try {
            addValueUnderEpoch(key, value);
        } finally {
            store.exitOp(ticket);
        }
    }

    private void addValueUnderEpoch(Value key, Value value) {
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

        WriteHandle handle = store.latchForWrite(currentId);
        try {
            LeafNode leaf = (LeafNode) handle.get();

            while (leaf.nextNode(key) != -1) {
                WriteHandle next = store.latchForWrite(leaf.nextNode(key));
                handle.close();
                handle = next;
                leaf = (LeafNode) handle.get();
            }

            boolean isRoot = rootRef.get() == leaf.id();
            CreationResult result = leaf.copyWith(key, value, idSupplier());

            while (result.promotedValue() != null) {
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
                    parentId = descendToLevel(result.promotedValue(), leftChild.height() + 1);
                }
                WriteHandle parent = latchLiveNode(parentId, result.oldFence());
                try {
                    BranchNode branchParent = (BranchNode) parent.get();

                    isRoot = rootRef.get() == branchParent.id();
                    result = branchParent.copyWith(
                            result.oldFence(), result.promotedValue(),
                            rightChild.id(), rightChild.biggestKey(), idSupplier());
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

    public void delete(Value key) {
        long ticket = store.enterOp();
        try {
            deleteUnderEpoch(key);
        } finally {
            store.exitOp(ticket);
        }
    }

    private void deleteUnderEpoch(Value key) {
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

        WriteHandle handle = store.latchForWrite(currentId);
        try {
            LeafNode leaf = (LeafNode) handle.get();
            while (leaf.nextNode(key) != -1) {
                WriteHandle next = store.latchForWrite(leaf.nextNode(key));
                handle.close();
                handle = next;
                leaf = (LeafNode) handle.get();
            }

            LeafNode updated = leaf.without(key);
            if (updated == null) {
                return;
            }
            handle.set(updated);

            Node current = updated;
            while (current.link() != -1 && current.isUnderfilled()) {
                Ascended ascended = tryAbsorbRight(handle, current, ancestors);
                if (ascended == null) {
                    break;
                }
                handle = ascended.handle;
                current = ascended.node;
            }
        } finally {
            handle.close();
        }
        collapseRootIfSingleChild();
    }

    private Ascended tryAbsorbRight(WriteHandle handle, Node current, Deque<Long> ancestors) {
        long rightId = current.link();

        WriteHandle rightHandle = store.latchForWrite(rightId);
        Node right = rightHandle.get();
        if (right.marked() || !current.canMergeWith(right)) {
            rightHandle.close();
            return null;
        }

        if (ancestors.isEmpty()) {
            rightHandle.close();
            return null;
        }
        Value rightFence = right.biggestKey();
        WriteHandle parentHandle = latchLiveNode(ancestors.peek(), rightFence);
        BranchNode parent = (BranchNode) parentHandle.get();
        Value[] parentKeys = parent.getKeys();
        int rightIdx = Arrays.binarySearch(parentKeys, rightFence);
        int curIdx = Arrays.binarySearch(parentKeys, current.biggestKey());
        if (rightIdx < 0 || curIdx < 0 || rightIdx != curIdx + 1) {
            parentHandle.close();
            rightHandle.close();
            return null;
        }

        long rightNodeId = right.id();
        int rightHeight = right.height();
        boolean rightIsLeaf = right.isLeaf();

        handle.set(current.mergeWithRight(right));
        rightHandle.set(rightIsLeaf
                ? LeafNode.tombstone(rightNodeId, rightHeight, current.id())
                : BranchNode.tombstone(rightNodeId, rightHeight, current.id()));
        store.retire(rightNodeId);
        rightHandle.close();

        BranchNode updatedParent = parent.removeDownlink(rightFence);
        if (updatedParent == null) {
            parentHandle.close();
            return null;
        }
        parentHandle.set(updatedParent);
        ancestors.pop();
        handle.close();
        return new Ascended(parentHandle, updatedParent);
    }

    private WriteHandle latchLiveNode(Long startId, Value guideKey) {
        WriteHandle h = store.latchForWrite(startId);
        Node n = h.get();
        while (n.marked() || n.shouldGoRight(guideKey)) {
            WriteHandle next = store.latchForWrite(n.link());
            h.close();
            h = next;
            n = h.get();
        }
        return h;
    }

    private record Ascended(WriteHandle handle, Node node) {}

    private void collapseRootIfSingleChild() {
        while (true) {
            long rootId = rootRef.get();
            Node root = store.readNode(rootId);
            if (root.isLeaf() || root.marked()) {
                return;
            }
            BranchNode branch = (BranchNode) root;
            if (branch.getPointers().length != 1) {
                return;
            }

            WriteHandle h = store.latchForWrite(rootId);
            try {
                if (rootRef.get() != rootId) {
                    continue;
                }
                BranchNode rootAgain = (BranchNode) h.get();
                if (rootAgain.marked() || rootAgain.getPointers().length != 1) {
                    return;
                }
                long child = rootAgain.getPointers()[0];
                h.set(BranchNode.tombstone(rootId, rootAgain.height(), child));
                store.retire(rootId);
                rootRef.set(child);
            } finally {
                h.close();
            }
        }
    }

    private void createNewRoot(Node leftChild, Node rightChild, Value promotedValue) {
        Node newRoot = store.writeNode(new BranchNode(
                        idSupplier().get(),
                        leftChild.height() + 1,
                        new Value[]{promotedValue, rightChild.biggestKey()},
                        new long[]{leftChild.id(), rightChild.id()},
                        -1L
                )
        );
        rootRef.set(newRoot.id());
    }

    private long descendToLevel(Value key, int targetHeight) {
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (node.height() > targetHeight) {
            currentId = node.nextNode(key);
            node = store.readNode(currentId);
        }
        return currentId;
    }

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
