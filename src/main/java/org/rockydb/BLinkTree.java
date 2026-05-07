package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.*;
import java.util.concurrent.locks.LockSupport;

public class BLinkTree {
    private final Store store;
    private final PerNodeLock nodeLock;
    private final List<Long> leftmostNodes;

    public BLinkTree(Store store) {
        this.store = store;
        this.nodeLock = new PerNodeLock();
        this.leftmostNodes = initLeftmostNodes();
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

        nodeLock.lockNode(currentId);
        LeafNode leaf = (LeafNode) store.readNode(currentId);

        while (leaf.nextNode(key) != -1) {
            long prevId = currentId;
            currentId = leaf.nextNode(key);
            nodeLock.lockNode(currentId);
            leaf = (LeafNode) store.readNode(currentId);
            nodeLock.unlockNode(prevId);
        }

        boolean isRoot = store.rootId() == leaf.id();
        CreationResult result = leaf.copyWith(key, value, store.nodeIdGenerator());

        while (result.promotedValue() != null) {
            Node rightChild = store.writeNode(result.right());
            Node leftChild = store.writeNode(result.left());
            if (isRoot) {
                createNewRoot(leftChild, rightChild, result.promotedValue());
                nodeLock.unlockNode(leftChild.id());
                result = null;
                break;
            }
            long parentId;
            if (!ancestors.isEmpty()) {
                parentId = ancestors.pop();
            } else {
                parentId = getLeftmostNodeAtLevel(leftChild.height());
            }

            nodeLock.lockNode(parentId);
            BranchNode parent = (BranchNode) store.readNode(parentId);

            while (parent.shouldGoRight(result.promotedValue())) {
                long prevId = parentId;
                parentId = parent.link();
                nodeLock.lockNode(parentId);
                parent = (BranchNode) store.readNode(parentId);
                nodeLock.unlockNode(prevId);
            }

            isRoot = store.rootId() == parent.id();
            result = parent.copyWith(result.promotedValue(), rightChild.id(), rightChild.biggestKey(), store.nodeIdGenerator());

            nodeLock.unlockNode(leftChild.id());
        }

        if (result != null) {
            Node left = store.writeNode(result.left());
            nodeLock.unlockNode(left.id());
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
        leftmostNodes.add(newRoot.id());
    }

    private List<Long> initLeftmostNodes() {
        long nodeId = store.rootId();
        Node node = store.readNode(nodeId);
        List<Long> leftmost = new ArrayList<>();
        leftmost.add(nodeId);
        while (!node.isLeaf() && ((BranchNode) node).getPointers().length > 0) {
            nodeId = ((BranchNode) node).getPointers()[0];
            leftmost.add(nodeId);
            node = store.readNode(nodeId);
        }
        return Collections.synchronizedList(leftmost.reversed());
    }

    private long getLeftmostNodeAtLevel(int level) {
        int tries = 0;
        while (tries < 5) {
            if (leftmostNodes.size() <= level) {
                LockSupport.parkNanos(1_000_000);
                tries++;
            } else {
                return leftmostNodes.get(level);
            }
        }
        throw new RuntimeException("Failed to get leftmost node at level " + level);
    }
}
