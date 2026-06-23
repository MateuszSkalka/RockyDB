package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.*;

public class BLinkTree {
    private final Store store;
    private final RootRef rootRef;

    public BLinkTree(Store store) {
        this(store, new StoreBackedRootRef(store));
    }

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
            CreationResult result = leaf.copyWith(key, value, store.nodeIdGenerator());

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
                WriteHandle parent = store.latchForWrite(parentId);
                try {
                    BranchNode parentNode = (BranchNode) parent.get();

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

    public void delete(Value key) {
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
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
