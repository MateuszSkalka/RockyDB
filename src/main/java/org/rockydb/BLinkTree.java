package org.rockydb;

import org.rockydb.Node.CreationResult;

import java.util.Arrays;
import java.util.Stack;

public class BLinkTree {
    private final NodeManager nodeManager;
    private final NodeLockSupport nodeLock;
    private long rootId;
    private int height;

    public BLinkTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.nodeLock = new NodeLockSupport();
        nodeManager.writeNode(new Node(
            null,
            new Value[]{new Value("T".getBytes())},
            new long[]{-78, -1},
            Node.LEAF,
            -1));
        rootId = 1L;
        height = 1;
    }

    protected long get(Value key) {
        Node node = nodeManager.readNode(rootId);

        while (node.getType() != Node.LEAF) {
            int idx = Arrays.binarySearch(node.getKeys(), key);
            idx = idx < 0 ? -(idx + 1) : idx;
            long nextId = node.getPointers()[idx];
            if (nextId == -1) { // rightmost node
                nextId = node.getPointers()[idx - 1];
            }
            node = nodeManager.readNode(nextId);
        }

        int idx = Arrays.binarySearch(node.getKeys(), key);
        if (idx > -1) {
            return node.getPointers()[idx];
        } else {
            return -1;
        }
    }


    protected void addValue(Value key, long value) {
        Stack<Long> nodes = new Stack<>();
        long currentId = rootId;
        int currentHeight = height;

        while (currentHeight > 1) {
            Node node = nodeManager.readNode(currentId);
            currentId = node.nextNode(key);

            if (!node.isRightLink(currentId)) {
                nodes.push(node.getId());
                currentHeight--;
            }
        }

        nodeLock.lockNode(currentId);
        Node leaf = nodeManager.readNode(currentId);
        while (leaf.shouldGoRight(key)) {
            long prevId = currentId;
            currentId = leaf.getLink();
            nodeLock.lockNode(currentId);
            leaf = nodeManager.readNode(currentId);
            nodeLock.unlockNode(prevId);
        }

        CreationResult result = leaf.withKey(key, value);

        while (result.promotedValue() != null && !nodes.isEmpty()) {
            Node rightChild = nodeManager.writeNode(result.right());
            result.left().setLink(rightChild.getId());
            Node leftChild = nodeManager.writeNode(result.left());

            currentId = nodes.pop();
            nodeLock.lockNode(currentId);
            Node parent = nodeManager.readNode(currentId);

            while (parent.shouldGoRight(result.promotedValue())) {
                long prevId = currentId;
                currentId = parent.getLink();
                nodeLock.lockNode(currentId);
                parent = nodeManager.readNode(currentId);
                nodeLock.unlockNode(prevId);
            }

            result = parent.withKey(result.promotedValue(), rightChild.getId());
            nodeLock.unlockNode(leftChild.getId());
        }

        if (result.promotedValue() == null) {
            nodeManager.writeNode(result.left());
            nodeLock.unlockNode(result.left().getId());
        } else {
            Node rightChild = nodeManager.writeNode(result.right());
            result.left().setLink(rightChild.getId());
            Node leftChild = nodeManager.writeNode(result.left());
            Node newRoot = nodeManager.writeNode(new Node(
                    null,
                    new Value[]{result.promotedValue(), rightChild.getKeys()[rightChild.getKeys().length - 1]},
                    new long[]{leftChild.getId(), rightChild.getId(), -1},
                    Node.BRANCH,
                    -1
                )
            );
            rootId = newRoot.getId();
            height++;
            nodeLock.unlockNode(leftChild.getId());
        }
    }

}
