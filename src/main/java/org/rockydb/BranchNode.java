package org.rockydb;

import java.util.Arrays;

public class BranchNode extends Node {
    private final Value[] keys;
    private final long[] pointers;

    public BranchNode(
        Long id,
        boolean isLeftmostNode,
        int height,
        Value[] keys,
        long[] valuePointers
    ) {
        super(id, false, isLeftmostNode, height);
        this.keys = keys;
        this.pointers = valuePointers;
    }

    @Override
    public long nextNode(Value key) {
        int idx = Arrays.binarySearch(keys, key);
        idx = idx < 0 ? -(idx + 1) : idx;
        return pointers[idx] == -1 ? pointers[idx - 1] : pointers[idx];
    }

    @Override
    public boolean isRightLink(long nodeId) {
        return pointers[pointers.length - 1] == nodeId;
    }

    @Override
    public boolean shouldGoRight(Value key) {
        return keys[keys.length - 1].compareTo(key) < 0 && pointers[pointers.length - 1] != -1;
    }

    @Override
    public long link() {
        return pointers[pointers.length - 1];
    }

    @Override
    public void setLink(long linkId) {
        pointers[pointers.length - 1] = linkId;
    }

    @Override
    public Value biggestKey() {
        return keys[keys.length - 1];
    }

    public Value[] getKeys() {
        return keys;
    }

    public long[] getPointers() {
        return pointers;
    }

    public CreationResult copyWith(Value key, long pointer, Value newMax) {
        compareAndSetBiggestKey(newMax);
        int idx = Arrays.binarySearch(keys, key);
        if (idx > -1) {
            pointers[idx] = pointer;
            return splitIfNeeded(keys, pointers);
        } else {
            idx = -(idx + 1);
            Value[] newKeys = insert(key, idx);
            long[] newPointers = insert(pointer, idx + 1);
            return splitIfNeeded(newKeys, newPointers);
        }
    }

    private CreationResult splitIfNeeded(Value[] keys, long[] pointers) {
        int newSize = size(keys) + size(pointers);
        if (needsSplit(newSize)) {
            return split(keys, pointers, newSize);
        } else {
            return new CreationResult(new BranchNode(id(), isLeftmostNode(), height(), keys, pointers), null, null);
        }
    }

    private CreationResult split(Value[] keys, long[] pointers, int size) {
        int keyMid = 0;
        int leftSize = sizeOfCell(0, keys);
        int rightSize = size - leftSize;
        while (
            keyMid < keys.length - 1 &&
                Math.abs(rightSize - leftSize) >
                    Math.abs((rightSize - sizeOfCell(keyMid + 1, keys)) - (leftSize + sizeOfCell(keyMid + 1, keys)))
        ) {
            keyMid++;
            leftSize += sizeOfCell(keyMid, keys);
            rightSize -= sizeOfCell(keyMid, keys);
        }

        Value promotedValue = keys[keyMid];
        Value[] leftKeys = new Value[keyMid + 1];
        long[] leftPointers = new long[leftKeys.length + 1];

        Value[] rightKeys = new Value[keys.length - leftKeys.length];
        long[] rightPointers = new long[rightKeys.length + 1];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length - 1);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, leftPointers.length - 1, rightPointers, 0, rightPointers.length);

        return new CreationResult(
            new BranchNode(id(), isLeftmostNode(), height(), leftKeys, leftPointers),
            new BranchNode(null, false, height(), rightKeys, rightPointers),
            promotedValue
        );
    }

    private void compareAndSetBiggestKey(Value key) {
        if (biggestKey().compareTo(key) < 0) {
            keys[keys.length - 1] = key;
        }
    }

    private Value[] insert(Value key, int idx) {
        Value[] newKeys = new Value[keys.length + 1];
        System.arraycopy(keys, 0, newKeys, 0, idx);
        newKeys[idx] = key;
        System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);
        return newKeys;
    }

    private long[] insert(long pointer, int idx) {
        long[] newPointers = new long[pointers.length + 1];
        System.arraycopy(pointers, 0, newPointers, 0, idx);
        newPointers[idx] = pointer;
        System.arraycopy(pointers, idx, newPointers, idx + 1, pointers.length - idx);
        return newPointers;
    }

    private int sizeOfCell(int keyIdx, Value[] keys) {
        return DiscStore.KEY_PREFIX_SIZE + keys[keyIdx].bytes().length + DiscStore.VALUE_POINTER_SIZE;

    }
}
