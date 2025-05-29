package org.rockydb;

public abstract class Node {
    public static final int MAX_NODE_SIZE = NodeManager.PAGE_SIZE - NodeManager.PAGE_HEADERS_SIZE;
    private Long id;
    private final boolean isLeaf;
    private final boolean isLeftmostNode;
    private final int height;

    public Node(Long id, boolean isLeaf, boolean isLeftmostNode, int height) {
        this.id = id;
        this.isLeaf = isLeaf;
        this.isLeftmostNode = isLeftmostNode;
        this.height = height;
    }

    public abstract long nextNode(Value key);

    public abstract boolean isRightLink(long nodeId);

    public abstract void setLink(long nodeId);

    public abstract boolean shouldGoRight(Value key);

    public abstract long link();

    public abstract Value biggestKey();

    public int height() {
        return height;
    }

    public boolean isRoot() {
        return isLeftmostNode() && isRightmostNode();
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public Long id() {
        return id;
    }

    public static boolean needsSplit(int nodeSize) {
        return nodeSize > MAX_NODE_SIZE;
    }

    public void setId(long id) {
        this.id = id;
    }

    protected boolean isRightmostNode() {
        return link() == -1;
    }

    protected boolean isLeftmostNode() {
        return isLeftmostNode;
    }

    protected int size(Value[] array) {
        int size = 0;
        for (Value value : array) {
            size += NodeManager.KEY_PREFIX_SIZE + value.bytes().length;
        }
        return size;
    }

    protected int size(long[] array) {
        return Long.BYTES * array.length;
    }

    public record CreationResult(
        Node left,
        Node right,
        Value promotedValue
    ) {
    }
}
