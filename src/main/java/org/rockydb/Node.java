package org.rockydb;

import java.util.Arrays;

public class Node {
    public static final byte UNUSED = 0;
    public static final byte BRANCH = 1;
    public static final byte LEAF = 2;

    private final NodeManager nodeManager;
    private long id;
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


    public SplitResult addValue(Value key, long value) {
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
            if (needsSplit(calcSizeAfterUpdate(key))) {
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
                if (needsSplit(calcSizeAfterUpdate(key))) {
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

    public long get(Value key) {
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
        return nodeSize > NodeManager.PAGE_SIZE;
    }

    private int calcSizeAfterUpdate(Value keyToAdd) {
        return size + Long.BYTES + Integer.BYTES + keyToAdd.val().length;
    }

    private SplitResult splitBranch() {
        int keyMid = keys.length / 2;
        int pointersMid = pointers.length / 2;
        Value promotedValue = keys[keyMid];

        Value[] leftKeys = new Value[keyMid];
        long[] leftPointers = new long[pointersMid];

        Value[] rightKeys = new Value[keys.length - keyMid - 1];
        long[] rightPointers = new long[pointers.length - pointersMid];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, pointersMid, rightPointers, 0, rightPointers.length);

        this.keys = leftKeys;
        this.pointers = leftPointers;
        Node rightNode = nodeManager.writeNode(BRANCH, rightKeys, rightPointers);
        return new SplitResult(promotedValue, this, rightNode);
    }

    private SplitResult splitLeaf() {
        int keyMid = keys.length / 2;
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

    public int getSize() {
        return size;
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
