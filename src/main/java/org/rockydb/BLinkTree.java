package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.LinkedList;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicReference;

public class BLinkTree {
    private final NodeManager nodeManager;
    private final PerNodeLock nodeLock;
    private final AtomicReference<RootInfo> rootInfo;
    private final LinkedList<Long> leftmostNodes = new LinkedList<>();

    public BLinkTree(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.nodeLock = new PerNodeLock();
        nodeManager.writeNode(new LeafNode(
            null,
            new Value[]{new Value("9999".getBytes())},
            new Value[]{new Value("9999".getBytes()), new Value(LongUtils.toByteArray(-1L))}));
        this.rootInfo = new AtomicReference<>(new RootInfo(1L, 1));
        leftmostNodes.add(1L);
    }

    protected Value get(Value key) {
        Node node = nodeManager.readNode(rootInfo.get().rootId);
        while (node.nextNode(key) != -1) {
            node = nodeManager.readNode(node.nextNode(key));
        }
        return ((LeafNode) node).getValueForKey(key);
    }


    protected void addValue(Value key, Value value) {
        Stack<Long> nodes = new Stack<>();
        RootInfo ri = this.rootInfo.get();
        long currentId = ri.rootId();
        int currentHeight = ri.height();
        System.out.println("rootId: " + ri.rootId() + ", height: " + ri.height());
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

        while (result != null && result.promotedValue() != null) {
            currentHeight++;
            long nodeToUpdate;
            if (!nodes.isEmpty()) {
                nodeToUpdate = nodes.pop();
            } else {
                nodeToUpdate = getLeftmostNodeAtHeight(currentHeight);
            }
            Node rightChild = nodeManager.writeNode(result.right());
            result.left().setLink(rightChild.id());
            Node leftChild = nodeManager.writeNode(result.left());
            if (nodeToUpdate != -1) {
                nodeLock.lockNode(nodeToUpdate);
                BranchNode parent = (BranchNode) nodeManager.readNode(nodeToUpdate);

                while (parent.shouldGoRight(result.promotedValue())) {
                    long prevId = nodeToUpdate;
                    nodeToUpdate = parent.link();
                    nodeLock.lockNode(nodeToUpdate);
                    parent = (BranchNode) nodeManager.readNode(nodeToUpdate);
                    nodeLock.unlockNode(prevId);
                }

                result = parent.copyWith(result.promotedValue(), rightChild.id(), rightChild.biggestKey());
            } else {
                synchronized (this) {
                    Node newRoot = nodeManager.writeNode(new BranchNode(
                            null,
                            new Value[]{result.promotedValue(), rightChild.biggestKey()},
                            new long[]{leftChild.id(), rightChild.id(), -1}
                        )
                    );
                    System.out.println("splitting root at height " + currentHeight + " new rootId: " + newRoot.id());
                    rootInfo.updateAndGet(old -> new RootInfo(newRoot.id(), old.height + 1));
                }
            }
            nodeLock.unlockNode(leftChild.id());
        }

        if (result != null) {
            Node left = nodeManager.writeNode(result.left());
            nodeLock.unlockNode(left.id());
        }
    }

    public long getLeftmostNodeAtHeight(int height) {
        RootInfo ri = this.rootInfo.get();
        int idx = ri.height() - height;
        if (idx < 0 || idx >= leftmostNodes.size()) {
            System.out.println("Not Found");
            return -1;
        }
        System.out.println("Found");
        return leftmostNodes.get(idx);
    }

    public record RootInfo(long rootId, int height) {
    }
}
