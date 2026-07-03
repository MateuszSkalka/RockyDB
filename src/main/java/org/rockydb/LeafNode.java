package org.rockydb;

import java.util.Arrays;
import java.util.function.Supplier;

public class LeafNode extends Node {
    private final Value[] keys;
    private final Value[] values;
    private final Value fence;

    public LeafNode(long id, int height, Value[] keys, Value[] values, long link) {
        this(id, height, keys, values, link, biggestOf(keys), false);
    }

    public LeafNode(long id, int height, Value[] keys, Value[] values, long link, boolean marked) {
        this(id, height, keys, values, link, biggestOf(keys), marked);
    }

    public LeafNode(long id, int height, Value[] keys, Value[] values, long link, Value fence, boolean marked) {
        super(id, true, height, link, marked);
        this.keys = keys;
        this.values = values;
        this.fence = fence;
    }

    @Override
    public boolean shouldGoRight(Value key) {
        if (marked()) return true;
        if (fence == null || link() == -1) return false;
        return key.compareTo(fence) > 0; // +inf fence: key < +inf always, so never go right
    }

    @Override
    public Value biggestKey() {
        return fence;
    }

    @Override
    public long nextNode(Value key) {
        if (marked()) return link();
        if (shouldGoRight(key)) return link();
        return -1;
    }

    @Override
    public boolean isRightLink(long nodeId) {
        return link() == nodeId;
    }

    public Value[] getKeys() {
        return keys;
    }

    public Value[] getValues() {
        return values;
    }

    public Value fence() {
        return fence;
    }

    public Value getValueForKey(Value key) {
        int idx = Arrays.binarySearch(keys, key);
        if (idx > -1) return values[idx];
        return null;
    }

    public CreationResult copyWith(Value keyToAdd, Value valueToAdd, Supplier<Long> nodeIdGenerator) {
        int idx = Arrays.binarySearch(keys, keyToAdd);
        if (idx > -1) {
            values[idx] = valueToAdd;
            return splitIfNeeded(keys, values, nodeIdGenerator);
        } else {
            idx = -(idx + 1);
            Value[] newKeys = insert(keys, keyToAdd, idx);
            Value[] newValues = insert(values, valueToAdd, idx);
            return splitIfNeeded(newKeys, newValues, nodeIdGenerator);
        }
    }

    public LeafNode without(Value key) {
        int idx = Arrays.binarySearch(keys, key);
        if (idx < 0) return null;
        return new LeafNode(id(), height(), remove(keys, idx), remove(values, idx), link(), fence, marked());
    }

    @Override
    public boolean isUnderfilled() {
        int body = size(keys) + size(values) + Store.LINK_POINTER_SIZE + fenceOverhead();
        return needsMerge(body);
    }

    @Override
    public boolean canMergeWith(Node right) {
        LeafNode r = (LeafNode) right;
        int merged = size(keys) + size(r.keys) + size(values) + size(r.values) + Store.LINK_POINTER_SIZE + r.fenceOverhead();
        return !needsSplit(merged);
    }

    @Override
    public Node mergeWithRight(Node right) {
        LeafNode r = (LeafNode) right;
        Value[] mergedKeys = concat(keys, r.keys);
        Value[] mergedValues = concat(values, r.values);
        return new LeafNode(id(), height(), mergedKeys, mergedValues, r.link(), r.fence, marked());
    }

    public static LeafNode tombstone(long id, int height, long survivorId) {
        return new LeafNode(id, height, new Value[0], new Value[0], survivorId, Value.infinity(), true);
    }

    /** Serialized size of the fence: prefix + bytes (0 for +inf). */
    int fenceOverhead() {
        return Store.KEY_PREFIX_SIZE + (fence == null || fence.isInfinite() ? 0 : fence.bytes().length);
    }

    private Value[] concat(Value[] left, Value[] right) {
        Value[] out = new Value[left.length + right.length];
        System.arraycopy(left, 0, out, 0, left.length);
        System.arraycopy(right, 0, out, left.length, right.length);
        return out;
    }

    private CreationResult splitIfNeeded(Value[] keys, Value[] values, Supplier<Long> nodeIdGenerator) {
        // Serialized body = keys + values + link + fence; size() already counts every key, so no
        // extra biggest-record term is needed. This matches isUnderfilled() and PageCodec exactly.
        int newSize = size(keys) + size(values) + Store.LINK_POINTER_SIZE + fenceOverhead();
        if (needsSplit(newSize)) {
            return split(keys, values, nodeIdGenerator);
        } else {
            return new CreationResult(new LeafNode(id(), height(), keys, values, link(), fence, marked()), null, null, null);
        }
    }

    private CreationResult split(Value[] keys, Value[] values, Supplier<Long> nodeIdGenerator) {
        int keyMid = 0;
        int leftSize = sizeOfCell(0, keys, values);
        int totalCells = size(keys) + size(values);
        int rightSize = totalCells - leftSize;
        while (
            keyMid < keys.length - 1 &&
                Math.abs(rightSize - leftSize) >
                    Math.abs((rightSize - sizeOfCell(keyMid + 1, keys, values)) - (leftSize + sizeOfCell(keyMid + 1, keys, values)))
        ) {
            keyMid++;
            leftSize += sizeOfCell(keyMid, keys, values);
            rightSize -= sizeOfCell(keyMid, keys, values);
        }

        Value promotedValue = keys[keyMid];
        Value[] leftKeys = new Value[keyMid + 1];
        Value[] leftValues = new Value[leftKeys.length];
        Value[] rightKeys = new Value[keys.length - leftKeys.length];
        Value[] rightValues = new Value[rightKeys.length];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(values, 0, leftValues, 0, leftKeys.length);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(values, leftKeys.length, rightValues, 0, rightKeys.length);

        Value biggestRecord = keys[keys.length - 1];
        Value rightFence = max(fence, biggestRecord);
        long rightNodeId = nodeIdGenerator.get();
        return new CreationResult(
            new LeafNode(id(), height(), leftKeys, leftValues, rightNodeId, promotedValue, marked()),
            new LeafNode(rightNodeId, height(), rightKeys, rightValues, this.link(), rightFence, marked()),
            promotedValue,
            fence
        );
    }

    private static Value biggestOf(Value[] keys) {
        return keys.length == 0 ? null : keys[keys.length - 1];
    }

    private static Value max(Value a, Value b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) >= 0 ? a : b;
    }

    private Value[] insert(Value[] array, Value e, int idx) {
        Value[] newArray = new Value[array.length + 1];
        System.arraycopy(array, 0, newArray, 0, idx);
        newArray[idx] = e;
        System.arraycopy(array, idx, newArray, idx + 1, array.length - idx);
        return newArray;
    }

    private Value[] remove(Value[] array, int idx) {
        Value[] newArray = new Value[array.length - 1];
        System.arraycopy(array, 0, newArray, 0, idx);
        System.arraycopy(array, idx + 1, newArray, idx, array.length - idx - 1);
        return newArray;
    }

    private int sizeOfCell(int keyIdx, Value[] keys, Value[] values) {
        return 2 * Store.KEY_PREFIX_SIZE + keys[keyIdx].bytes().length + values[keyIdx].bytes().length;
    }
}
