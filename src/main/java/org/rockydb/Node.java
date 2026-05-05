package org.rockydb;

public abstract class Node {
    public static final int MAX_NODE_SIZE = DiscStore.PAGE_SIZE - DiscStore.PAGE_HEADERS_SIZE;
    private final long id;
    private final boolean isLeaf;
    private final boolean isLeftmostNode;
    private final int height;
    private final long link;

    public Node(Long id, boolean isLeaf, boolean isLeftmostNode, int height, long link) {
        this.id = id;
        this.isLeaf = isLeaf;
        this.isLeftmostNode = isLeftmostNode;
        this.height = height;
        this.link = link;
    }

    public abstract long nextNode(Value key);

    public abstract boolean isRightLink(long nodeId);

    public abstract boolean shouldGoRight(Value key);

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

    protected boolean isRightmostNode() {
        return link() == -1;
    }

    protected boolean isLeftmostNode() {
        return isLeftmostNode;
    }

    protected int size(Value[] array) {
        int size = 0;
        for (Value value : array) {
            size += DiscStore.KEY_PREFIX_SIZE + value.bytes().length;
        }
        return size;
    }

    public long link() {
        return link;
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
