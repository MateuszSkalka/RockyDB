package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

public class BLinkTree {
    private final Store store;
    private final PerNodeLock nodeLock;
    private final Map<Integer, Long> leftmostNodes = new ConcurrentHashMap<>();

    public BLinkTree(Store store) {
        this.store = store;
        this.nodeLock = new PerNodeLock();
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
        Stack<Long> nodes = new Stack<>();
        long currentId = store.rootId();
        Node node = store.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
            if (!node.isRightLink(currentId)) {
                nodes.push(node.id());
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

        boolean isRoot = leaf.isRoot();
        CreationResult result = leaf.copyWith(key, value);

        while (result.promotedValue() != null) {
            Node rightChild = store.writeNode(result.right());
            result.left().setLink(rightChild.id());
            Node leftChild = store.writeNode(result.left());
            if (isRoot) {
                createNewBranchRoot(leftChild, rightChild, result.promotedValue());
                nodeLock.unlockNode(leftChild.id());
                result = null;
                break;
            }
            long parentId;
            if (!nodes.isEmpty()) {
                parentId = nodes.pop();
            } else {
                parentId = getLeftmostNodeAtHeight(leftChild.height() + 1);
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

            isRoot = parent.isRoot();
            result = parent.copyWith(result.promotedValue(), rightChild.id(), rightChild.biggestKey());

            nodeLock.unlockNode(leftChild.id());
        }

        if (result != null) {
            Node left = store.writeNode(result.left());
            nodeLock.unlockNode(left.id());
        }
    }

    private void createNewBranchRoot(Node leftChild, Node rightChild, Value promotedValue) {
        Node newRoot = store.writeNode(new BranchNode(
                null,
                true,
                leftChild.height() + 1,
                new Value[]{promotedValue, rightChild.biggestKey()},
                new long[]{leftChild.id(), rightChild.id(), -1}
            )
        );
        store.updateRootId(newRoot.id());
        leftmostNodes.put(newRoot.height(), newRoot.id());
    }

    private long getLeftmostNodeAtHeight(int height) {
        int tries = 0;
        while (tries < 5) {
            Long id = leftmostNodes.get(height);
            if (id != null) {
                return id;
            } else {
                LockSupport.parkNanos(1_000_000);
                tries++;
            }
        }
        throw new RuntimeException("Failed to get leftmost node at height " + height);
    }
}
