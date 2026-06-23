package org.rockydb;

import java.nio.ByteBuffer;

import static org.rockydb.ByteUtils.readIsLeafFlag;

public final class PageCodec {

    private PageCodec() {}

    public static Node deserialize(long id, ByteBuffer buffer) {
        byte flags = buffer.get();
        boolean isLeaf = readIsLeafFlag(flags);
        int elemCount = buffer.getShort();
        int height = buffer.getShort();

        if (isLeaf) {
            return readLeafNode(id, height, buffer, elemCount);
        } else {
            return readBranchNode(id, height, buffer, elemCount);
        }
    }

    public static ByteBuffer serialize(Node node) {
        if (node instanceof BranchNode branchNode) {
            return createBuffer(node.isLeaf(), node.height(), branchNode.getKeys(), branchNode.getPointers(), branchNode.link());
        } else if (node instanceof LeafNode leafNode) {
            return createBuffer(node.isLeaf(), node.height(), leafNode.getKeys(), leafNode.getValues(), leafNode.link());
        } else {
            throw new IllegalArgumentException("Unsupported node type: " + node.getClass());
        }
    }

    private static LeafNode readLeafNode(long id, int height, ByteBuffer buffer, int elemCount) {
        Value[] keys = readValueArray(buffer, elemCount);
        Value[] values = readValueArray(buffer, elemCount);
        long link = buffer.getLong();
        return new LeafNode(id, height, keys, values, link);
    }

    private static BranchNode readBranchNode(long id, int height, ByteBuffer buffer, int elemCount) {
        Value[] keys = readValueArray(buffer, elemCount);
        long[] values = readLongArray(buffer, elemCount);
        long link = buffer.getLong();
        return new BranchNode(id, height, keys, values, link);
    }

    private static Value[] readValueArray(ByteBuffer buffer, int size) {
        Value[] arr = new Value[size];
        for (int i = 0; i < arr.length; i++) {
            int nextSize = buffer.getInt();
            byte[] bytes = new byte[nextSize];
            buffer.get(bytes);
            arr[i] = new Value(bytes);
        }
        return arr;
    }

    private static long[] readLongArray(ByteBuffer buffer, int size) {
        long[] arr = new long[size];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = buffer.getLong();
        }
        return arr;
    }

    private static ByteBuffer createBuffer(boolean isLeaf, int height, Value[] keys, long[] pointers, long link) {
        ByteBuffer buffer = createBuffer(isLeaf, keys.length, height);

        for (Value key : keys) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        for (long valuePointer : pointers) {
            buffer.putLong(valuePointer);
        }
        buffer.putLong(link);
        return buffer;
    }

    private static ByteBuffer createBuffer(boolean isLeaf, int height, Value[] keys, Value[] values, long link) {
        ByteBuffer buffer = createBuffer(isLeaf, keys.length, height);

        for (Value key : keys) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        for (Value key : values) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        buffer.putLong(link);
        return buffer;
    }

    private static ByteBuffer createBuffer(boolean isLeaf, int numOfKeys, int height) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        // is-leaf flag
        buffer.put(ByteUtils.createFlags(isLeaf));
        // number of keys
        buffer.putShort((short) numOfKeys);
        // height
        buffer.putShort((short) height);
        return buffer;
    }
}
