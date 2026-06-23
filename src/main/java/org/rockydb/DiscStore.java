package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

final class DiscStore implements AutoCloseable {
    private static final long TREE_ROOT_FILE_POSITION = 0;

    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final AtomicLong nextPageId;
    private final AtomicLong rootId;

    DiscStore(File dbFile) throws IOException {
        raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        this.nextPageId = new AtomicLong(loadNextPageId());
        this.rootId = new AtomicLong(loadRootId());
        checkAndInitTree();
    }

    ByteBuffer readRawPage(long id) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        try {
            fileChannel.read(buffer, id * Store.PAGE_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.rewind();
        return buffer;
    }

    void writeRawPage(long id, ByteBuffer buffer) {
        buffer.rewind();
        try {
            fileChannel.write(buffer, Store.PAGE_SIZE * id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Supplier<Long> nodeIdGenerator() {
        return nextPageId::getAndIncrement;
    }

    void updateRootId(long id) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        buffer.putLong(id);
        buffer.rewind();
        try {
            fileChannel.write(buffer, TREE_ROOT_FILE_POSITION);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rootId.set(id);
    }

    long rootId() {
        return rootId.get();
    }

    private long loadNextPageId() throws IOException {
        return Math.max(1, raf.length() / Store.PAGE_SIZE);
    }

    private long loadRootId() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        fileChannel.read(buffer, TREE_ROOT_FILE_POSITION);
        buffer.rewind();
        long val = buffer.getLong();
        if (val < 1) return -1;
        else return val;
    }

    private void checkAndInitTree() {
        if (rootId.get() == -1) {
            long id = nextPageId.getAndIncrement();
            Node root = new LeafNode(id, 1, new Value[]{}, new Value[]{}, -1L);
            writeRawPage(id, PageCodec.serialize(root));
            updateRootId(id);
        }
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
}
