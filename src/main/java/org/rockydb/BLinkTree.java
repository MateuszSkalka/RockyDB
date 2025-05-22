package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.Stack;

public class BLinkTree {
    private final NodeManager nodeManager;
    private final NodeLockSupport nodeLock;
    private long rootId;
    private int height;

    public BLinkTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.nodeLock = new NodeLockSupport();
        nodeManager.writeNode(new LeafNode(
            null,
            new Value[]{new Value("root".getBytes())},
            new Value[]{new Value("root".getBytes()), new Value(LongUtils.toByteArray(-1L))}));
        rootId = 1L;
        height = 1;
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
        int currentHeight = height;

        while (currentHeight > 1) {
            Node node = nodeManager.readNode(currentId);
            currentId = node.nextNode(key);

            if (!node.isRightLink(currentId)) {
                nodes.push(node.id());
                currentHeight--;
            }
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

        CreationResult result = leaf.copyWith(key, value);

        while (result.promotedValue() != null && !nodes.isEmpty()) {
            Node rightChild = nodeManager.writeNode(result.right());
            result.left().setLink(rightChild.id());
            Node leftChild = nodeManager.writeNode(result.left());

            currentId = nodes.pop();
            nodeLock.lockNode(currentId);
            BranchNode parent = (BranchNode) nodeManager.readNode(currentId);

            while (parent.shouldGoRight(result.promotedValue())) {
                long prevId = currentId;
                currentId = parent.link();
                nodeLock.lockNode(currentId);
                parent = (BranchNode) nodeManager.readNode(currentId);
                nodeLock.unlockNode(prevId);
            }

            result = parent.copyWith(result.promotedValue(), rightChild.id(), rightChild.biggestKey());
            nodeLock.unlockNode(leftChild.id());
        }

        if (result.promotedValue() == null) {
            nodeManager.writeNode(result.left());
            nodeLock.unlockNode(result.left().id());
        } else {
            Node rightChild = nodeManager.writeNode(result.right());
            result.left().setLink(rightChild.id());
            Node leftChild = nodeManager.writeNode(result.left());
            Node newRoot = nodeManager.writeNode(new BranchNode(
                    null,
                    new Value[]{result.promotedValue(), rightChild.biggestKey()},
                    new long[]{leftChild.id(), rightChild.id(), -1}
                )
            );
            rootId = newRoot.id();
            height++;
            nodeLock.unlockNode(leftChild.id());
        }
    }

}
