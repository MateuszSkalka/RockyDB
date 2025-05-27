package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class NodeManager implements AutoCloseable {
    public static final int PAGE_SIZE = 4 * 1024;
    public static final int PAGE_HEADERS_SIZE = 5;
    public static final int KEY_PREFIX_SIZE = 4;
    public static final int VALUE_POINTER_SIZE = 8;
    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final FileHeaders fileHeaders;
    private final StripedLock stripedLock;

    public NodeManager(File dbFile) throws IOException {
        raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        this.fileHeaders = new FileHeaders();
        this.stripedLock = new StripedLock();

    }

    public Node readNode(long id) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        stripedLock.runInReadLock(id, () -> {
            try {
                fileChannel.read(buffer, id * PAGE_SIZE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        buffer.rewind();

        byte nodeType = buffer.get();
        int elemCount = buffer.getInt();

        return switch (nodeType) {
            case Node.BRANCH -> readBranchNode(id, buffer, elemCount);
            case Node.LEAF -> readLeafNode(id, buffer, elemCount);
            default -> throw new RuntimeException();
        };
    }

    private LeafNode readLeafNode(long id, ByteBuffer buffer, int elemCount) {
        Value[] keys = readValueArray(buffer, elemCount);
        Value[] values = readValueArray(buffer, elemCount + 1);
        return new LeafNode(id, keys, values);
    }

    private BranchNode readBranchNode(long id, ByteBuffer buffer, int elemCount) {
        Value[] keys = readValueArray(buffer, elemCount);
        long[] values = readLongArray(buffer, elemCount + 1);
        return new BranchNode(id, keys, values);
    }

    private Value[] readValueArray(ByteBuffer buffer, int size) {
        Value[] arr = new Value[size];
        for (int i = 0; i < arr.length; i++) {
            int nextSize = buffer.getInt();
            byte[] bytes = new byte[nextSize];
            buffer.get(bytes);
            arr[i] = new Value(bytes);
        }
        return arr;
    }

    private long[] readLongArray(ByteBuffer buffer, int size) {
        long[] arr = new long[size];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = buffer.getLong();
        }
        return arr;
    }


    public LeafNode writeNode(LeafNode node) {
        ByteBuffer buffer = createBuffer(node.type(), node.getKeys(), node.getValues());
        long nodeId = node.id() == null ? fileHeaders.incrementAndGetPageCount() : node.id();
        buffer.rewind();
        stripedLock.runInWriteLock(nodeId, () -> {
            try {
                fileChannel.write(buffer, PAGE_SIZE * nodeId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return new LeafNode(nodeId, node.getKeys(), node.getValues());
    }

    public Node writeNode(Node node) {
        try {
            ByteBuffer buffer;
            if (node instanceof BranchNode branchNode) {
                buffer = createBuffer(node.type(), branchNode.getKeys(), branchNode.getPointers());
            } else if (node instanceof LeafNode leafNode) {
                buffer = createBuffer(node.type(), leafNode.getKeys(), leafNode.getValues());
            } else {
                throw new IllegalArgumentException();
            }

            long nodeId = node.id() == null ? fileHeaders.incrementAndGetPageCount() : node.id();
            buffer.rewind();
            fileChannel.write(buffer, PAGE_SIZE * nodeId);

            node.setId(nodeId);
            return node;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private ByteBuffer createBuffer(byte type, Value[] keys, long[] pointers) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        buffer.put(type);
        buffer.putInt(keys.length);

        for (Value key : keys) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        for (long valuePointer : pointers) {
            buffer.putLong(valuePointer);
        }
        return buffer;
    }

    private ByteBuffer createBuffer(byte type, Value[] keys, Value[] values) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        buffer.put(type);
        buffer.putInt(keys.length);

        for (Value key : keys) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        for (Value key : values) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        return buffer;
    }

    @Override
    public void close() throws Exception {
        if (fileChannel != null) {
            fileChannel.close();
        }
        if (raf != null) {
            raf.close();
        }
    }

    private class FileHeaders {
        public static final int FILE_HEADERS_SIZE = 16;

        private long rootId;
        private AtomicLong pagesCount;

        public FileHeaders() throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
            fileChannel.read(buffer, 0);
            buffer.rewind();
            rootId = buffer.getLong();
            pagesCount = new AtomicLong(buffer.getLong());
        }

        public long incrementAndGetPageCount() {
            return pagesCount.incrementAndGet();
        }
    }
}
