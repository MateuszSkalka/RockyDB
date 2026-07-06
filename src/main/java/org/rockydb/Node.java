package org.rockydb;

import static org.rockydb.Store.MERGE_THRESHOLD;

public abstract sealed class Node permits BranchNode, LeafNode {
    public static final int MAX_NODE_SIZE = Store.PAGE_SIZE - Store.PAGE_HEADERS_SIZE;
    private final long id;
    private final boolean isLeaf;
    private final int height;
    private final long link;
    private final boolean marked;

    public Node(long id, boolean isLeaf, int height, long link) {
        this(id, isLeaf, height, link, false);
    }

    public Node(long id, boolean isLeaf, int height, long link, boolean marked) {
        this.id = id;
        this.isLeaf = isLeaf;
        this.height = height;
        this.link = link;
        this.marked = marked;
    }

    public abstract long nextNode(Value key);

    public abstract boolean isRightLink(long nodeId);

    public abstract boolean shouldGoRight(Value key);

    public abstract Value biggestKey();

    public abstract boolean isUnderfilled();

    public abstract boolean canMergeWith(Node right);

    public abstract Node mergeWithRight(Node right);

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

    public static boolean needsMerge(int nodeSize) {
        return nodeSize < (int) (MERGE_THRESHOLD * MAX_NODE_SIZE);
    }

    protected int size(Value[] array) {
        int size = 0;
        for (Value value : array) {
            size += Store.KEY_PREFIX_SIZE + (value == null || value.isInfinite() ? 0 : value.bytes().length);
        }
        return size;
    }

    public long link() {
        return link;
    }

    public boolean marked() {
        return marked;
    }

    protected int size(long[] array) {
        return Long.BYTES * array.length;
    }

    public record CreationResult(
        Node left,
        Node right,
        Value promotedValue,
        Value oldFence
    ) {
    }
}
