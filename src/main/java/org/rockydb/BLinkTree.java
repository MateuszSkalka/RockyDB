package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.locks.LockSupport;

public class BLinkTree {
    private final NodeManager nodeManager;
    private final PerNodeLock nodeLock;
    private volatile long rootId;
    private final LinkedList<Long> leftmostNodes = new LinkedList<>();

    public BLinkTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.nodeLock = new PerNodeLock();
        nodeManager.writeNode(new LeafNode(
            null,
            true,
            (short) 1,
            new Value[]{new Value("9999".getBytes())},
            new Value[]{new Value("9999".getBytes()), new Value(ByteUtils.toByteArray(-1L))}));
        this.rootId = 1L;
        leftmostNodes.add(1L);
    }

    protected Value get(Value key) {
        Node node = nodeManager.readNode(rootId);
        while (node.nextNode(key) != -1) {
            node = nodeManager.readNode(node.nextNode(key));
        }
        return ((LeafNode) node).getValueForKey(key);
    }


    protected void addValue(Value key, Value value) {
        Stack<Long> nodes = new Stack<>();
        long currentId = rootId;
        Node node = nodeManager.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
            if (!node.isRightLink(currentId)) {
                nodes.push(node.id());
            }
            node = nodeManager.readNode(currentId);
        }

        nodeLock.lockNode(currentId);
        LeafNode leaf = (LeafNode) nodeManager.readNode(currentId);

        while (leaf.nextNode(key) != -1) {
            long prevId = currentId;
            currentId = leaf.nextNode(key);
            nodeLock.lockNode(currentId);
            leaf = (LeafNode) nodeManager.readNode(currentId);
            nodeLock.unlockNode(prevId);
        }

        boolean isRoot = leaf.isRoot();
        CreationResult result = leaf.copyWith(key, value);

        while (result.promotedValue() != null) {
            Node rightChild = nodeManager.writeNode(result.right());
            result.left().setLink(rightChild.id());
            Node leftChild = nodeManager.writeNode(result.left());
            if (isRoot) {
                createNewRoot(leftChild, rightChild, result.promotedValue());
                nodeLock.unlockNode(leftChild.id());
                result = null;
                break;
            }
            long parentId;
            if (!nodes.isEmpty()) {
                parentId = nodes.pop();
            } else {
                parentId = getLeftmostNodeAtHeight(leftChild.height());
            }

            nodeLock.lockNode(parentId);
            BranchNode parent = (BranchNode) nodeManager.readNode(parentId);

            while (parent.shouldGoRight(result.promotedValue())) {
                long prevId = parentId;
                parentId = parent.link();
                nodeLock.lockNode(parentId);
                parent = (BranchNode) nodeManager.readNode(parentId);
                nodeLock.unlockNode(prevId);
            }

            isRoot = parent.isRoot();
            result = parent.copyWith(result.promotedValue(), rightChild.id(), rightChild.biggestKey());

            nodeLock.unlockNode(leftChild.id());
        }

        if (result != null) {
            Node left = nodeManager.writeNode(result.left());
            nodeLock.unlockNode(left.id());
        }
    }

    public void createNewRoot(Node leftChild, Node rightChild, Value promotedValue) {
        Node newRoot = nodeManager.writeNode(new BranchNode(
                null,
                true,
                leftChild.height() + 1,
                new Value[]{promotedValue, rightChild.biggestKey()},
                new long[]{leftChild.id(), rightChild.id(), -1}
            )
        );
        rootId = newRoot.id();
        leftmostNodes.add(rootId);
    }

    public long getLeftmostNodeAtHeight(int height) {
        int tries = 0;
        while (tries++ < 5) {
            try {
                return leftmostNodes.get(height);
            } catch (IndexOutOfBoundsException e) {
                LockSupport.parkNanos(1_000_000);
            }
        }
        throw new RuntimeException("Failed to get leftmost node at height " + height);
    }
}
