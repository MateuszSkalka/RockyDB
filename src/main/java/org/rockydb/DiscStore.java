package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public final class DiscStore implements AutoCloseable {
    private static final long TREE_ROOT_FILE_POSITION = 0;       // 8 bytes: data tree root id
    private static final long FREE_LOG_HEAD_FILE_POSITION = 8;   // 8 bytes: packed (headPageId, headSlot)
    private static final long EPOCH_FILE_POSITION = 16;          // 8 bytes: persisted epoch counter
    private static final long FREE_LOG_TAIL_FILE_POSITION = 24;  // 8 bytes: packed (tailPageId, tailCount)
    private static final long COUNT_FILE_POSITION = 32;          // 8 bytes: element count of the header-rooted tree

    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final AtomicLong rootId;
    private final AtomicLong nextPageId;

    DiscStore(File dbFile) throws IOException {
        raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        this.rootId = new AtomicLong(loadRootId());
        this.nextPageId = new AtomicLong(Math.max(1L, loadFilePages()));
    }

    long allocateRaw() {
        return nextPageId.getAndIncrement();
    }

    long highWaterMark() {
        return nextPageId.get();
    }

    long freeLogHeadPacked() {
        return readLongUnchecked(FREE_LOG_HEAD_FILE_POSITION);
    }

    long freeLogTailPacked() {
        return readLongUnchecked(FREE_LOG_TAIL_FILE_POSITION);
    }

    void writeFreeLogBootstrap(long headPacked, long tailPacked) {
        writeLong(FREE_LOG_HEAD_FILE_POSITION, headPacked);
        writeLong(FREE_LOG_TAIL_FILE_POSITION, tailPacked);
    }

    long loadEpoch() {
        return Math.max(0, readLongUnchecked(EPOCH_FILE_POSITION));
    }

    void writeEpoch(long epoch) {
        writeLong(EPOCH_FILE_POSITION, epoch);
    }

    long loadCount() {
        return Math.max(0, readLongUnchecked(COUNT_FILE_POSITION));
    }

    void writeCount(long count) {
        writeLong(COUNT_FILE_POSITION, count);
    }

    ByteBuffer readRawPage(long id) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        readFully(id * Store.PAGE_SIZE, buffer);
        buffer.rewind();
        return buffer;
    }

    /** Reads the page straight into {@code dst} — no temp buffer, no copy. */
    void readRawPage(long id, byte[] dst) {
        ByteBuffer buffer = ByteBuffer.wrap(dst);
        readFully(id * Store.PAGE_SIZE, buffer);
    }

    /**
     * Positional reads are not guaranteed to fill the buffer in one call; loop.
     * A short read at EOF (page never written) reads back as zeros.
     */
    private void readFully(long position, ByteBuffer buffer) {
        try {
            while (buffer.hasRemaining()) {
                int n = fileChannel.read(buffer, position + buffer.position());
                if (n < 0) {
                    Arrays.fill(buffer.array(), buffer.position(), buffer.capacity(), (byte) 0);
                    buffer.position(buffer.capacity());
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void writeRawPage(long id, ByteBuffer buffer) {
        buffer.rewind();
        long position = Store.PAGE_SIZE * id;
        try {
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer, position + buffer.position());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void updateRootId(long id) {
        writeLong(TREE_ROOT_FILE_POSITION, id);
        rootId.set(id);
    }

    long rootId() {
        return rootId.get();
    }

    private long readLongUnchecked(long position) {
        try {
            return readLong(position);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long readLong(long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        fileChannel.read(buffer, position);
        buffer.rewind();
        return buffer.getLong();
    }

    private void writeLong(long position, long value) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        buffer.putLong(value);
        buffer.rewind();
        try {
            fileChannel.write(buffer, position);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long loadFilePages() throws IOException {
        return Math.max(1, raf.length() / Store.PAGE_SIZE);
    }

    private long loadRootId() throws IOException {
        long val = readLong(TREE_ROOT_FILE_POSITION);
        return val < 1 ? -1 : val;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
        raf.close();
    }
}
