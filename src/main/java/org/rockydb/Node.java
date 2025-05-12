package org.rockydb;

import org.checkerframework.checker.units.qual.C;

import java.util.Arrays;

public class Node {
    public static final byte UNUSED = 0;
    public static final byte BRANCH = 1;
    public static final byte LEAF = 2;
    public static final int MAX_NODE_SIZE = NodeManager.PAGE_SIZE;

    private final Long id;
    private final Value[] keys;
    private final long[] pointers;
    private final byte type;
    private final int size;


    public Node(
        Long id,
        Value[] keys,
        long[] valuePointers,
        byte type,
        int size
    ) {
        this.id = id;
        this.keys = keys;
        this.pointers = valuePointers;
        this.type = type;
        this.size = size;
    }

    public CreationResult withKey(Value key, long value) {
        int idx = Arrays.binarySearch(keys, key);
        if (idx > -1) {
            pointers[idx] = value;
            return new CreationResult(new Node(id, keys, pointers, type, size), null, null);
        } else {
            idx = -(idx + 1);
            Value[] newKeys = copyWithNewValue(key, idx);
            long[] newPointers = copyWithNewPointer(value, idx);

            int newSize = sizeAfterUpdate(key);
            if (Node.needsSplit(newSize)) {
                int keyMid = 1;
                int leftSize = NodeManager.PAGE_HEADERS_SIZE + sizeOfCell(0) + sizeOfCell(1);
                int rightSize = size - leftSize;
                while (
                    keyMid < keys.length - 2 &&
                        Math.abs(rightSize - leftSize) > Math.abs((rightSize - sizeOfCell( keyMid + 1)) - (leftSize + sizeOfCell(keyMid + 1)))
                ) {
                    keyMid++;
                    leftSize += sizeOfCell(keyMid);
                    rightSize -= sizeOfCell(keyMid);
                }

                Value promotedValue = newKeys[keyMid];

                Value[] leftKeys = new Value[keyMid + 1];
                long[] leftPointers = new long[leftKeys.length + 1];

                Value[] rightKeys = new Value[newKeys.length - leftKeys.length];
                long[] rightPointers = new long[rightKeys.length + 1];

                System.arraycopy(newKeys, 0, leftKeys, 0, leftKeys.length);
                System.arraycopy(newPointers, 0, leftPointers, 0, leftPointers.length - 1);
                System.arraycopy(newKeys, keyMid + 1, rightKeys, 0, rightKeys.length);
                System.arraycopy(newPointers, leftPointers.length - 1, rightPointers, 0, rightPointers.length);

                return new CreationResult(
                    new Node(id, leftKeys, leftPointers, type, leftSize),
                    new Node(null, rightKeys, rightPointers, type, rightSize),
                    promotedValue
                );
            } else {
                return new CreationResult(new Node(id, newKeys, newPointers, type, newSize), null, null);
            }
        }
    }

    public long nextNode(Value key) {
        int idx = Arrays.binarySearch(keys, key);
        idx = idx < 0 ? -(idx + 1) : idx;
        return pointers[idx] == -1 ? pointers[idx - 1] : pointers[idx];
    }

    public boolean isRightLink(long nodeId) {
        return pointers[pointers.length - 1] == nodeId;
    }

    public boolean shouldGoRight(Value key) {
        return keys[keys.length - 1].compareTo(key) < 0 && pointers[pointers.length - 1] != -1;
    }

    public long getLink() {
        return pointers[pointers.length - 1];
    }

    public void setLink(long linkId) {
        pointers[pointers.length - 1] = linkId;
    }

    public static boolean needsSplit(int nodeSize) {
        return nodeSize > MAX_NODE_SIZE;
    }

    public int sizeAfterUpdate(Value keyToAdd) {
        return size + NodeManager.VALUE_POINTER_SIZE + NodeManager.KEY_PREFIX_SIZE + keyToAdd.val().length;
    }

    private Value[] copyWithNewValue(Value key, int idx) {
        Value[] newKeys = new Value[keys.length + 1];
        System.arraycopy(keys, 0, newKeys, 0, idx);
        newKeys[idx] = key;
        System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);
        return newKeys;
    }

    private long[] copyWithNewPointer(long pointer, int idx) {
        long[] newPointers = new long[pointers.length + 1];
        System.arraycopy(pointers, 0, newPointers, 0, idx);
        newPointers[idx] = pointer;
        System.arraycopy(pointers, idx, newPointers, idx + 1, pointers.length - idx);
        return newPointers;
    }

    private int sizeOfCell(int keyIdx) {
        return NodeManager.KEY_PREFIX_SIZE + keys[keyIdx].val().length + NodeManager.VALUE_POINTER_SIZE;

    }

    public Long getId() {
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

    public record CreationResult(
        Node left,
        Node right,
        Value promotedValue
    ) {
    }
}
