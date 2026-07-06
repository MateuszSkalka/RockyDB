package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

final class DiscStore implements AutoCloseable {
    private static final long TREE_ROOT_FILE_POSITION = 0;       // 8 bytes: data tree root id
    private static final long FREE_LOG_HEAD_FILE_POSITION = 8;   // 8 bytes: packed (headPageId, headSlot)
    private static final long EPOCH_FILE_POSITION = 16;          // 8 bytes: persisted epoch counter
    private static final long FREE_LOG_TAIL_FILE_POSITION = 24;  // 8 bytes: packed (tailPageId, tailCount)

    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final AtomicLong rootId;
    private final AtomicLong nextPageId;
    private final PageAllocator allocator;

    DiscStore(File dbFile) throws IOException {
        raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        this.rootId = new AtomicLong(loadRootId());
        this.nextPageId = new AtomicLong(Math.max(1L, loadFilePages()));

        long headPacked;
        long tailPacked;
        if (rootId.get() == -1) {
            long dataRootId = allocateRaw();
            Node dataRoot = new LeafNode(dataRootId, 1, new Value[]{}, new Value[]{}, -1L, Value.infinity(), false);
            writeRawPage(dataRootId, PageCodec.serialize(dataRoot));
            updateRootId(dataRootId);

            long freePageId = allocateRaw();
            writeRawPage(freePageId, FreeLog.emptyPage());
            headPacked = FreeLog.pack(freePageId, 0);
            tailPacked = headPacked;
            writeFreeLogHead(headPacked);
            writeFreeLogTail(tailPacked);
        } else {
            headPacked = freeLogHeadPacked();
            tailPacked = freeLogTailPacked();
        }

        this.allocator = new PageAllocator(loadEpoch(), new DiscFreeLogIO(), this::allocateRaw, headPacked, tailPacked);
        writeEpoch(allocator.currentEpoch());
    }

    long allocateRaw() {
        return nextPageId.getAndIncrement();
    }

    long highWaterMark() {
        return nextPageId.get();
    }

    long freeLogHeadPacked() {
        try {
            return readLong(FREE_LOG_HEAD_FILE_POSITION);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    long freeLogTailPacked() {
        try {
            return readLong(FREE_LOG_TAIL_FILE_POSITION);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    PageAllocator allocator() {
        return allocator;
    }

    void updateRootId(long id) {
        writeLong(TREE_ROOT_FILE_POSITION, id);
        rootId.set(id);
    }

    long rootId() {
        return rootId.get();
    }

    private void writeFreeLogHead(long packed) {
        writeLong(FREE_LOG_HEAD_FILE_POSITION, packed);
    }

    private void writeFreeLogTail(long packed) {
        writeLong(FREE_LOG_TAIL_FILE_POSITION, packed);
    }

    private void writeEpoch(long epoch) {
        writeLong(EPOCH_FILE_POSITION, epoch);
    }

    private long loadEpoch() throws IOException {
        return Math.max(0, readLong(EPOCH_FILE_POSITION));
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
    public void close() throws Exception {
        writeEpoch(allocator.currentEpoch());
        if (fileChannel != null) {
            fileChannel.close();
        }
        if (raf != null) {
            raf.close();
        }
    }

    private class DiscFreeLogIO implements FreeLog.FreeLogIO {

        @Override
        public ByteBuffer readPage(long pageId) {
            return readRawPage(pageId);
        }

        @Override
        public void writePage(long pageId, ByteBuffer buffer) {
            writeRawPage(pageId, buffer);
        }

        @Override
        public void writeBootstrap(long headPacked, long tailPacked) {
            writeLong(FREE_LOG_HEAD_FILE_POSITION, headPacked);
            writeLong(FREE_LOG_TAIL_FILE_POSITION, tailPacked);
        }

        @Override
        public long allocateRaw() {
            return DiscStore.this.allocateRaw();
        }

        @Override
        public long currentEpoch() {
            return allocator.currentEpoch();
        }
    }
}
