package org.rockydb;

public abstract class Node {
    public static final int MAX_NODE_SIZE = Store.PAGE_SIZE - Store.PAGE_HEADERS_SIZE;
    private final long id;
    private final boolean isLeaf;
    private final int height;
    private final long link;
    private final boolean marked;

    public Node(long id, boolean isLeaf, int height, long link) {
        this(id, isLeaf, height, link, false);
    }

    /**
     * @param marked when true, this node has been merged away and {@code link} no longer points to a
     *               right sibling but to the surviving node — readers must follow {@code link()} as a
     *               redirect (see {@link LeafNode#nextNode}). Such "tombstone" nodes are never split
     *               and are not reclaimed until a safe-drain mechanism exists.
     */
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

    /**
     * Whether this node's serialized body is below the merge threshold — a candidate to be absorbed
     * by its right sibling on a delete cascade. Implementations use the same byte accounting as
     * {@link #needsSplit}.
     */
    public abstract boolean isUnderfilled();

    /**
     * Whether this node and {@code right} (assumed to be this node's current right sibling) would fit
     * together in a single node. A leaf is only ever merged with a leaf, a branch only with a branch.
     */
    public abstract boolean canMergeWith(Node right);

    /**
     * Returns a new node that absorbs {@code right}'s contents into this node, keeping this node's id
     * (the survivor) and taking {@code right}'s right link. {@code right} must subsequently be replaced
     * by a tombstone.
     */
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
        return nodeSize < (int) (Store.MERGE_THRESHOLD * MAX_NODE_SIZE);
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
