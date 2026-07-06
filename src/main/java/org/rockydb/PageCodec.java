package org.rockydb;

import java.nio.ByteBuffer;

import static org.rockydb.ByteUtils.readIsLeafFlag;
import static org.rockydb.ByteUtils.readMarkedFlag;

public final class PageCodec {

    private PageCodec() {}

    public static Node deserialize(long id, ByteBuffer buffer) {
        byte flags = buffer.get();
        boolean isLeaf = readIsLeafFlag(flags);
        boolean marked = readMarkedFlag(flags);
        int elemCount = buffer.getShort();
        int height = buffer.getShort();

        if (isLeaf) {
            return readLeafNode(id, height, buffer, elemCount, marked);
        } else {
            return readBranchNode(id, height, buffer, elemCount, marked);
        }
    }

    public static ByteBuffer serialize(Node node) {
        byte flags = ByteUtils.createFlags(node.isLeaf(), node.marked());
        if (node instanceof BranchNode branchNode) {
            return createBuffer(flags, node.height(), branchNode.getKeys(), branchNode.getPointers(), branchNode.link());
        } else if (node instanceof LeafNode leafNode) {
            return createBuffer(flags, node.height(), leafNode.getKeys(), leafNode.getValues(), leafNode.link(), leafNode.fence());
        } else {
            throw new IllegalArgumentException("Unsupported node type: " + node.getClass());
        }
    }

    private static LeafNode readLeafNode(long id, int height, ByteBuffer buffer, int elemCount, boolean marked) {
        Value[] keys = readValueArray(buffer, elemCount);
        Value[] values = readValueArray(buffer, elemCount);
        long link = buffer.getLong();
        Value fence = readFence(buffer);
        return new LeafNode(id, height, keys, values, link, fence, marked);
    }

    private static BranchNode readBranchNode(long id, int height, ByteBuffer buffer, int elemCount, boolean marked) {
        Value[] keys = readValueArray(buffer, elemCount);
        long[] values = readLongArray(buffer, elemCount);
        long link = buffer.getLong();
        return new BranchNode(id, height, keys, values, link, marked);
    }

    private static Value[] readValueArray(ByteBuffer buffer, int size) {
        Value[] arr = new Value[size];
        for (int i = 0; i < arr.length; i++) {
            int len = buffer.getInt();
            if (len == -1) {
                arr[i] = Value.infinity();
            } else {
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                arr[i] = new Value(bytes);
            }
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

    private static void writeValue(ByteBuffer buffer, Value v) {
        if (v == null || v.isInfinite()) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(v.bytes().length);
            buffer.put(v.bytes());
        }
    }

    private static Value readFence(ByteBuffer buffer) {
        int len = buffer.getInt();
        if (len == -1) return Value.infinity();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new Value(bytes);
    }

    private static ByteBuffer createBuffer(byte flags, int height, Value[] keys, long[] pointers, long link) {
        ByteBuffer buffer = createBuffer(flags, keys.length, height);
        for (Value key : keys) writeValue(buffer, key);
        for (long p : pointers) buffer.putLong(p);
        buffer.putLong(link);
        return buffer;
    }

    private static ByteBuffer createBuffer(byte flags, int height, Value[] keys, Value[] values, long link, Value fence) {
        ByteBuffer buffer = createBuffer(flags, keys.length, height);
        for (Value key : keys) writeValue(buffer, key);
        for (Value v : values) writeValue(buffer, v);
        buffer.putLong(link);
        writeValue(buffer, fence);
        return buffer;
    }

    private static ByteBuffer createBuffer(byte flags, int numOfKeys, int height) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        // is-leaf + marked flags
        buffer.put(flags);
        // number of keys
        buffer.putShort((short) numOfKeys);
        // height
        buffer.putShort((short) height);
        return buffer;
    }
}
