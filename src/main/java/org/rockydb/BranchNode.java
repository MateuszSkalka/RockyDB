package org.rockydb;

import java.util.Arrays;
import java.util.function.Supplier;

public final class BranchNode extends Node {
    private final Value[] keys;
    private final long[] pointers;

    public BranchNode(
            Long id,
            int height,
            Value[] keys,
            long[] valuePointers,
            long link
    ) {
        this(id, height, keys, valuePointers, link, false);
    }

    public BranchNode(
            Long id,
            int height,
            Value[] keys,
            long[] valuePointers,
            long link,
            boolean marked
    ) {
        super(id, false, height, link, marked);
        this.keys = keys;
        this.pointers = valuePointers;
    }

    @Override
    public long nextNode(Value key) {
        if (marked()) return link();
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
        if (marked()) return true;
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

    public CreationResult copyWith(Value oldFence, Value promotedValue, long newChildId, Value newChildMax, Supplier<Long> nodeIdGenerator) {
        int idx = Arrays.binarySearch(keys, oldFence);
        if (idx < 0) {
            throw new IllegalStateException("old fence not found in branch " + id());
        }
        Value[] newKeys = new Value[keys.length + 1];
        System.arraycopy(keys, 0, newKeys, 0, idx);
        newKeys[idx] = promotedValue;
        newKeys[idx + 1] = newChildMax;
        System.arraycopy(keys, idx + 1, newKeys, idx + 2, keys.length - idx - 1);

        long[] newPointers = new long[pointers.length + 1];
        System.arraycopy(pointers, 0, newPointers, 0, idx + 1);
        newPointers[idx + 1] = newChildId;
        System.arraycopy(pointers, idx + 1, newPointers, idx + 2, pointers.length - idx - 1);

        return splitIfNeeded(newKeys, newPointers, nodeIdGenerator);
    }

    public BranchNode removeDownlink(Value fence) {
        int idx = Arrays.binarySearch(keys, fence);
        if (idx < 0 || pointers.length == 1) {
            return null;
        }
        int keyToRemove = (idx == 0) ? 0 : idx - 1;
        return new BranchNode(id(), height(), remove(keys, keyToRemove), remove(pointers, idx), link());
    }

    @Override
    public boolean isUnderfilled() {
        int body = size(keys) + size(pointers) + Store.LINK_POINTER_SIZE;
        return needsMerge(body);
    }

    @Override
    public boolean canMergeWith(Node right) {
        BranchNode r = (BranchNode) right;
        int merged = size(keys) + size(r.keys) + size(pointers) + size(r.pointers) + Store.LINK_POINTER_SIZE;
        return !needsSplit(merged);
    }

    @Override
    public Node mergeWithRight(Node right) {
        BranchNode r = (BranchNode) right;
        Value[] mergedKeys = concat(keys, r.keys);
        long[] mergedPointers = concat(pointers, r.pointers);
        return new BranchNode(id(), height(), mergedKeys, mergedPointers, r.link());
    }

    public static BranchNode tombstone(long id, int height, long survivorId) {
        return new BranchNode(id, height, new Value[0], new long[0], survivorId, true);
    }

    private Value[] concat(Value[] left, Value[] right) {
        Value[] out = new Value[left.length + right.length];
        System.arraycopy(left, 0, out, 0, left.length);
        System.arraycopy(right, 0, out, left.length, right.length);
        return out;
    }

    private long[] concat(long[] left, long[] right) {
        long[] out = new long[left.length + right.length];
        System.arraycopy(left, 0, out, 0, left.length);
        System.arraycopy(right, 0, out, left.length, right.length);
        return out;
    }

    private CreationResult splitIfNeeded(Value[] keys, long[] pointers, Supplier<Long> nodeIdGenerator) {
        int newSize = size(keys) + size(pointers) + Store.LINK_POINTER_SIZE;
        if (needsSplit(newSize)) {
            return split(keys, pointers, newSize, nodeIdGenerator);
        } else {
            return new CreationResult(new BranchNode(id(), height(), keys, pointers, link()), null, null, null);
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
                new BranchNode(id(), height(), leftKeys, leftPointers, rightNodeId),
                new BranchNode(rightNodeId, height(), rightKeys, rightPointers, link()),
                promotedValue,
                keys[keys.length - 1]
        );
    }

    private Value[] remove(Value[] array, int idx) {
        Value[] out = new Value[array.length - 1];
        System.arraycopy(array, 0, out, 0, idx);
        System.arraycopy(array, idx + 1, out, idx, array.length - idx - 1);
        return out;
    }

    private long[] remove(long[] array, int idx) {
        long[] out = new long[array.length - 1];
        System.arraycopy(array, 0, out, 0, idx);
        System.arraycopy(array, idx + 1, out, idx, array.length - idx - 1);
        return out;
    }

    private int sizeOfCell(int keyIdx, Value[] keys) {
        return Store.KEY_PREFIX_SIZE + (keys[keyIdx].isInfinite() ? 0 : keys[keyIdx].bytes().length) + Store.VALUE_POINTER_SIZE;
    }
}
