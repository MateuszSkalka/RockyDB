package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeManager implements AutoCloseable {
    public static final int PAGE_SIZE = 4 * 1024;
    public static final int PAGE_HEADERS_SIZE = 9;
    public static final int KEY_PREFIX_SIZE = 4;
    public static final int VALUE_POINTER_SIZE = 8;
    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final FileHeaders fileHeaders;

    public NodeManager(File dbFile) throws IOException {
        raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        this.fileHeaders = new FileHeaders();

    }

    public Node readNode(long id) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
            fileChannel.read(buffer, id * PAGE_SIZE);
            buffer.rewind();

            byte nodeType = buffer.get();
            int elemCount = buffer.getInt();
            int pageSize = buffer.getInt();

            Value[] keys = new Value[elemCount];
            int pSize = nodeType == Node.LEAF ? elemCount : elemCount + 1;
            long[] valuePointers = new long[pSize];

            for (int i = 0; i < elemCount; i++) {
                int nextValueSize = buffer.getInt();
                byte[] bytes = new byte[nextValueSize];
                buffer.get(bytes);
                keys[i] = new Value(bytes);
            }

            for (int i = 0; i < valuePointers.length; i++) {
                valuePointers[i] = buffer.getLong();
            }

            return new Node(this, id, keys, valuePointers, nodeType, pageSize);
        } catch (IOException ex) {
            throw new RuntimeException();
        }
    }

    public Node writeNode(byte type, Value[] keys, long[] valuePointers) {
        try {
            ByteBuffer buffer = createBuffer(type, keys, valuePointers);
            long nodeId = fileHeaders.incrementAndGetPageCount();
            int size = buffer.position();
            buffer.rewind();
            fileChannel.write(buffer, PAGE_SIZE * nodeId);
            return new Node(this, nodeId, keys, valuePointers, type, size);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public Node writeNode(Node node) {
        try {
            ByteBuffer buffer = createBuffer(node.getType(), node.getKeys(), node.getPointers());
            node.setSize(buffer.position());
            buffer.rewind();
            fileChannel.write(buffer, PAGE_SIZE * node.getId());
            return node;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private ByteBuffer createBuffer(byte type, Value[] keys, long[] valuePointers) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        buffer.put(type);
        buffer.putInt(keys.length);
        buffer.putInt(-1);

        for (Value key : keys) {
            buffer.putInt(key.val().length);
            buffer.put(key.val());
        }
        for (long valuePointer : valuePointers) {
            buffer.putLong(valuePointer);
        }
        buffer.putInt(5, buffer.position());
        return buffer;
    }

    public boolean anyNodeExists() {
        return fileHeaders.pagesCount.get() > 0L;
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
