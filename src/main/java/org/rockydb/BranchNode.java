package org.rockydb;

import java.util.Arrays;
import java.util.function.Supplier;

public class BranchNode extends Node {
    private final Value[] keys;
    private final long[] pointers;

    public BranchNode(
            Long id,
            boolean isLeftmostNode,
            int height,
            Value[] keys,
            long[] valuePointers,
            long link
    ) {
        super(id, false, isLeftmostNode, height, link);
        this.keys = keys;
        this.pointers = valuePointers;
    }

    @Override
    public long nextNode(Value key) {
        int idx = Arrays.binarySearch(keys, key);
        idx = idx < 0 ? -(idx + 1) : idx;
        if (idx == pointers.length && link() != -1) {
            return link();
        } else if (idx == pointers.length) {
            return pointers[idx - 1];
        } else {
            return pointers[idx];
        }
    }

    @Override
    public boolean isRightLink(long nodeId) {
        return link() == nodeId;
    }

    @Override
    public boolean shouldGoRight(Value key) {
        return keys[keys.length - 1].compareTo(key) < 0 && link() != -1;
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

    public CreationResult copyWith(Value key, long pointer, Value newMax, Supplier<Long> nodeIdGenerator) {
        compareAndSetBiggestKey(newMax);
        int idx = Arrays.binarySearch(keys, key);
        if (idx > -1) {
            pointers[idx] = pointer;
            return splitIfNeeded(keys, pointers, nodeIdGenerator);
        } else {
            idx = -(idx + 1);
            Value[] newKeys = insert(key, idx);
            long[] newPointers = insert(pointer, idx + 1);
            return splitIfNeeded(newKeys, newPointers, nodeIdGenerator);
        }
    }

    private CreationResult splitIfNeeded(Value[] keys, long[] pointers, Supplier<Long> nodeIdGenerator) {
        int newSize = size(keys) + size(pointers) + Store.LINK_POINTER_SIZE;
        if (needsSplit(newSize)) {
            return split(keys, pointers, newSize, nodeIdGenerator);
        } else {
            return new CreationResult(new BranchNode(id(), isLeftmostNode(), height(), keys, pointers, link()), null, null);
        }
    }

    private CreationResult split(Value[] keys, long[] pointers, int size, Supplier<Long> nodeIdGenerator) {
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
        long[] leftPointers = new long[leftKeys.length];

        Value[] rightKeys = new Value[keys.length - leftKeys.length];
        long[] rightPointers = new long[rightKeys.length];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, leftPointers.length, rightPointers, 0, rightPointers.length);

        long rightNodeId = nodeIdGenerator.get();
        return new CreationResult(
                new BranchNode(id(), isLeftmostNode(), height(), leftKeys, leftPointers, rightNodeId),
                new BranchNode(rightNodeId, false, height(), rightKeys, rightPointers, link()),
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
