package org.rockydb;

import java.util.Arrays;

public class Node {
    public static final byte UNUSED = 0;
    public static final byte BRANCH = 1;
    public static final byte LEAF = 2;
    public static final int MAX_NODE_SIZE = NodeManager.PAGE_SIZE;
    public static final int MAX_DATA_SIZE = NodeManager.PAGE_SIZE - NodeManager.PAGE_HEADERS_SIZE;
    public static final int MIN_NODE_SIZE = MAX_NODE_SIZE / 4;

    private final NodeManager nodeManager;
    private final long id;
    private Value[] keys;
    private long[] pointers;
    private final byte type;
    private int size;


    public Node(
        NodeManager nodeManager,
        long id,
        Value[] keys,
        long[] valuePointers,
        byte type,
        int size
    ) {
        this.nodeManager = nodeManager;
        this.id = id;
        this.keys = keys;
        this.pointers = valuePointers;
        this.type = type;
        this.size = size;
    }


    protected SplitResult addValue(Value key, long value) {
        int idx = Arrays.binarySearch(keys, key);

        if (type == BRANCH) {
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            long nextId = pointers[idx];
            SplitResult result = nodeManager.readNode(nextId).addValue(key, value);
            if (result == null) {
                return null;
            }

            Value[] newKeys = new Value[keys.length + 1];
            System.arraycopy(keys, 0, newKeys, 0, idx);
            newKeys[idx] = result.key;
            System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);

            long[] newPointers = new long[pointers.length + 1];
            System.arraycopy(pointers, 0, newPointers, 0, idx + 1);
            newPointers[idx + 1] = result.right.id;
            System.arraycopy(pointers, idx + 1, newPointers, idx + 2, pointers.length - idx - 1);

            this.keys = newKeys;
            this.pointers = newPointers;

            SplitResult sr = null;
            if (needsSplit(updateSize(key))) {
                sr = splitBranch();
            }

            nodeManager.writeNode(this);
            if (sr != null) {
                nodeManager.writeNode(sr.right);
            }

            return sr;

        } else if (type == LEAF) {
            if (idx > -1) {
                pointers[idx] = value;
                nodeManager.writeNode(this);
                return null;
            } else {
                idx = -(idx + 1);

                Value[] newKeys = new Value[keys.length + 1];
                System.arraycopy(keys, 0, newKeys, 0, idx);
                newKeys[idx] = key;
                System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);

                long[] newPointers = new long[keys.length + 1];
                System.arraycopy(pointers, 0, newPointers, 0, idx);
                newPointers[idx] = value;
                System.arraycopy(pointers, idx, newPointers, idx + 1, keys.length - idx);

                this.keys = newKeys;
                this.pointers = newPointers;

                SplitResult result = null;
                if (needsSplit(updateSize(key))) {
                    result = splitLeaf();
                }

                nodeManager.writeNode(this);

                if (result != null) {
                    nodeManager.writeNode(result.right);
                }

                return result;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    protected SplitResult deleteValue(Value key) {
        int idx = Arrays.binarySearch(keys, key);

        if (type == BRANCH) {
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            long nextId = pointers[idx];
            SplitResult result = nodeManager.readNode(nextId).deleteValue(key);
            return null;

        } else if (type == LEAF) {
            if (idx > -1) {
                Value[] newKeys = new Value[keys.length - 1];
                System.arraycopy(keys, 0, newKeys, 0, idx);
                System.arraycopy(keys, idx + 1, newKeys, idx, newKeys.length);

                long[] newPointers = new long[keys.length - 1];
                System.arraycopy(pointers, 0, newPointers, 0, idx);
                System.arraycopy(pointers, idx + 1, newPointers, idx, newPointers.length);
                this.keys = newKeys;
                this.pointers = newPointers;

                nodeManager.writeNode(this);
                return null;
            } else {
                return null;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    protected long get(Value key) {
        int idx = Arrays.binarySearch(keys, key);

        if (type == BRANCH) {
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            long nextId = pointers[idx];
            return nodeManager.readNode(nextId).get(key);
        } else if (type == LEAF) {
            if (idx > -1) {
                return pointers[idx];
            } else {
                return -1;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private boolean needsSplit(int nodeSize) {
        return nodeSize > MAX_NODE_SIZE;
    }

    private int updateSize(Value keyToAdd) {
        size += NodeManager.VALUE_POINTER_SIZE + NodeManager.KEY_PREFIX_SIZE + keyToAdd.val().length;
        return size;
    }

    private SplitResult splitBranch() {
        int keyMid = calculateBranchNodeSplitMid();

        Value promotedValue = keys[keyMid];

        Value[] leftKeys = new Value[keyMid];
        long[] leftPointers = new long[keyMid + 1];

        Value[] rightKeys = new Value[keys.length - leftKeys.length - 1];
        long[] rightPointers = new long[pointers.length - leftPointers.length];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, leftPointers.length, rightPointers, 0, rightPointers.length);

        this.keys = leftKeys;
        this.pointers = leftPointers;
        Node rightNode = nodeManager.writeNode(BRANCH, rightKeys, rightPointers);
        return new SplitResult(promotedValue, this, rightNode);
    }

    private SplitResult splitLeaf() {
        int keyMid = calculateLeafNodeSplitMid();
        int pointersMid = pointers.length / 2;

        Value[] leftKeys = new Value[keyMid];
        long[] leftPointers = new long[pointersMid];

        Value[] rightKeys = new Value[keys.length - keyMid];
        long[] rightPointers = new long[pointers.length - pointersMid];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length);
        System.arraycopy(keys, keyMid, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, pointersMid, rightPointers, 0, rightPointers.length);

        this.keys = leftKeys;
        this.pointers = leftPointers;
        Node rightNode = nodeManager.writeNode(LEAF, rightKeys, rightPointers);
        return new SplitResult(rightNode.keys[0], this, rightNode);
    }

    private int calculateBranchNodeSplitMid() {
        int keyMid = 1;
        int leftSize = sizeOfCell(0);
        int rightSize = size - NodeManager.PAGE_HEADERS_SIZE - leftSize - sizeOfCell(keyMid);
        while (
            keyMid < keys.length - 2 &&
                Math.abs(rightSize - leftSize) > Math.abs((rightSize - sizeOfCell(keyMid + 1)) - (leftSize + sizeOfCell(keyMid)))
        ) {
            leftSize += sizeOfCell(keyMid);
            rightSize -= sizeOfCell(keyMid + 1);
            keyMid++;
        }
        return keyMid;
    }

    private int calculateLeafNodeSplitMid() {
        int keyMid = 1;
        int leftSize = sizeOfCell(0);
        int rightSize = size - NodeManager.PAGE_HEADERS_SIZE - leftSize;
        while (
            keyMid < keys.length - 2 &&
                Math.abs(rightSize - leftSize) > Math.abs((rightSize - sizeOfCell(keyMid)) - (leftSize + sizeOfCell(keyMid)))
        ) {
            leftSize += sizeOfCell(keyMid);
            rightSize -= sizeOfCell(keyMid);
            keyMid++;
        }
        return keyMid;
    }

    private int sizeOfCell(int keyIdx) {
        return NodeManager.KEY_PREFIX_SIZE + keys[keyIdx].val().length + NodeManager.VALUE_POINTER_SIZE;

    }

    public long getId() {
        return id;
    }

    public Value[] getKeys() {
        return keys;
    }

    public long[] getPointers() {
        return pointers;
    }

    public byte getType() {
        return type;
    }

    public void setSize(int size) {
        this.size = size;
    }

    static class SplitResult {
        Value key;
        Node left;
        Node right;

        public SplitResult(Value key, Node left, Node right) {
            this.key = key;
            this.left = left;
            this.right = right;
        }
    }
}
