package org.rockydb;

public abstract class Node {
    public static final byte BRANCH = 1;
    public static final byte LEAF = 2;
    public static final int MAX_NODE_SIZE = NodeManager.PAGE_SIZE - NodeManager.PAGE_HEADERS_SIZE;
    private Long id;
    private final byte type;

    public Node(Long id, byte type) {
        this.id = id;
        this.type = type;
    }

    public abstract long nextNode(Value key);

    public abstract boolean isRightLink(long nodeId);

    public abstract void setLink(long nodeId);

    public abstract boolean shouldGoRight(Value key);

    public abstract long link();

    public abstract Value biggestKey();

    public byte type() {
        return type;
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
