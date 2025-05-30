package org.rockydb;

import java.util.Arrays;

public class LeafNode extends Node {
    private final Value[] keys;
    private final Value[] values;

    public LeafNode(
        Long id,
        boolean isLeftmostNode,
        int height,
        Value[] keys,
        Value[] values
    ) {
        super(id, true, isLeftmostNode, height);
        this.keys = keys;
        this.values = values;
    }

    @Override
    public boolean shouldGoRight(Value key) {
        if (keys.length == 0) return false;
        return keys[keys.length - 1].compareTo(key) < 0 && link() != -1;
    }

    @Override
    public long link() {
        return ByteUtils.toLong(values[values.length - 1].bytes());
    }

    @Override
    public Value biggestKey() {
        return keys[keys.length - 1];
    }

    @Override
    public void setLink(long linkId) {
        values[values.length - 1] = new Value(ByteUtils.toByteArray(linkId));
    }

    @Override
    public long nextNode(Value key) {
        if (shouldGoRight(key)) return link();
        else return -1;
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

    public Value getValueForKey(Value key) {
        int idx = Arrays.binarySearch(keys, key);
        if (idx > -1) return values[idx];
        else return null;
    }

    public CreationResult copyWith(Value keyToAdd, Value valueToAdd) {
        int idx = Arrays.binarySearch(keys, keyToAdd);
        if (idx > -1) {
            values[idx] = valueToAdd;
            return splitIfNeeded(keys, values);
        } else {
            idx = -(idx + 1);
            Value[] newKeys = insert(keys, keyToAdd, idx);
            Value[] newValues = insert(values, valueToAdd, idx);
            return splitIfNeeded(newKeys, newValues);
        }
    }

    private CreationResult splitIfNeeded(Value[] keys, Value[] values) {
        int newSize = size(keys) + size(values);
        if (needsSplit(newSize)) {
            return split(keys, values, newSize);
        } else {
            return new CreationResult(new LeafNode(id(), isLeftmostNode(), height(), keys, values), null, null);
        }
    }

    private CreationResult split(Value[] keys, Value[] values, int newSize) {
        int keyMid = 0;
        int leftSize = sizeOfCell(0, keys, values);
        int rightSize = newSize - leftSize;
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
        Value[] leftValues = new Value[leftKeys.length + 1];

        Value[] rightKeys = new Value[keys.length - leftKeys.length];
        Value[] rightValues = new Value[rightKeys.length + 1];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(values, 0, leftValues, 0, leftValues.length - 1);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(values, leftValues.length - 1, rightValues, 0, rightValues.length);

        return new CreationResult(
            new LeafNode(id(), isLeftmostNode(), height(), leftKeys, leftValues),
            new LeafNode(null, false, height(), rightKeys, rightValues),
            promotedValue
        );
    }

    private Value[] insert(Value[] array, Value e, int idx) {
        Value[] newArray = new Value[array.length + 1];
        System.arraycopy(array, 0, newArray, 0, idx);
        newArray[idx] = e;
        System.arraycopy(array, idx, newArray, idx + 1, array.length - idx);
        return newArray;
    }

    private int sizeOfCell(int keyIdx, Value[] keys, Value[] values) {
        return 2 * DiscStore.KEY_PREFIX_SIZE + keys[keyIdx].bytes().length + values[keyIdx].bytes().length;
    }
}