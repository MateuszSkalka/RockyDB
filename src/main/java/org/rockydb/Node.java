package org.rockydb;

public abstract class Node {
    public static final int MAX_NODE_SIZE = DiscStore.PAGE_SIZE - DiscStore.PAGE_HEADERS_SIZE;
    private final long id;
    private final boolean isLeaf;
    private final int height;
    private final long link;

    public Node(long id, boolean isLeaf, int height, long link) {
        this.id = id;
        this.isLeaf = isLeaf;
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

    public boolean isLeaf() {
        return isLeaf;
    }

    public long id() {
        return id;
    }

    public static boolean needsSplit(int nodeSize) {
        return nodeSize > MAX_NODE_SIZE;
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
