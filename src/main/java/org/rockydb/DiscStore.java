package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Raw disc access layer for {@link BufferedPool}: positional read/write of fixed
 * {@link Store#PAGE_SIZE} pages plus root-id metadata.
 * <p>
 * Package-private on purpose — {@link BufferedPool} is the only {@link Store} implementation.
 * It serializes all disc I/O through its per-frame {@code ioLock}, so this class performs no
 * locking of its own and must only be reached via {@code BufferedPool}. Callers of the raw
 * methods are responsible for holding the relevant frame's {@code ioLock} to avoid torn reads
 * and writes.
 */
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

    /**
     * Read a raw {@code PAGE_SIZE} page from disc into a fresh buffer (positioned at 0).
     * Package-private: used by {@link BufferedPool} to fill cache frames. The caller must hold
     * the relevant frame's {@code ioLock} to avoid a torn read.
     */
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

    /**
     * Write a full {@code PAGE_SIZE} page to disc at the page's offset. The buffer is rewound
     * first and {@code PAGE_SIZE} bytes are written. Package-private: used by {@link
     * BufferedPool} to flush dirty frames. The caller must hold the relevant frame's
     * {@code ioLock} to avoid a torn write.
     */
    void writeRawPage(long id, ByteBuffer buffer) {
        buffer.rewind();
        try {
            fileChannel.write(buffer, Store.PAGE_SIZE * id);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Backs {@link BufferedPool#nodeIdGenerator()}: monotonically increasing page ids (>= 1). */
    Supplier<Long> nodeIdGenerator() {
        return nextPageId::getAndIncrement;
    }

    /** Backs {@link BufferedPool#updateRootId(long)}: writes the root id to page 0. */
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

    /** Backs {@link BufferedPool#rootId()}. */
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
