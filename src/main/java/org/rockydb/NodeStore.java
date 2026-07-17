package org.rockydb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Node-level storage: turns the raw frames of a {@link BufferedPool} into {@link Node}s
 * via {@link PageCodec}, provides tree latching through {@link WriteHandle}, tracks the
 * tree root id, and recycles pages safely ({@link EpochManager} + {@link FreeLog}):
 * a retired page is re-allocated only once no in-flight operation can reference it.
 * Bootstraps an empty database file (root leaf + free-log page) on first open.
 */
public class NodeStore implements Store, Closeable {

    private final DiscStore discStore;
    private final BufferedPool pool;
    private final EpochManager epochs;
    private final FreeLog freeLog;

    public NodeStore(File dbFile, int numFrames) throws IOException {
        this.discStore = new DiscStore(dbFile);
        this.pool = new BufferedPool(discStore, numFrames);
        if (discStore.rootId() == -1) {
            bootstrap();
        }
        this.epochs = new EpochManager(discStore.loadEpoch() + 1);
        this.freeLog = new FreeLog(
                discStore, epochs, discStore.freeLogHeadPacked(), discStore.freeLogTailPacked());
    }

    private void bootstrap() {
        long dataRootId = discStore.allocateRaw();
        Node dataRoot = new LeafNode(dataRootId, 1, new Value[]{}, new Value[]{}, -1L, Value.infinity(), false);
        discStore.writeRawPage(dataRootId, PageCodec.serialize(dataRoot));
        discStore.updateRootId(dataRootId);

        long freePageId = discStore.allocateRaw();
        discStore.writeRawPage(freePageId, FreeLog.emptyPage());
        long packed = FreeLog.pack(freePageId, 0);
        discStore.writeFreeLogBootstrap(packed, packed);
    }

    @Override
    public Node readNode(long id) {
        Frame frame = pool.acquire(id);
        frame.ioLock.readLock().lock();
        try {
            frame.bumpUsage();
            return PageCodec.deserialize(id, ByteBuffer.wrap(frame.bytes));
        } finally {
            frame.ioLock.readLock().unlock();
            frame.unpin();
        }
    }

    @Override
    public Node writeNode(Node node) {
        Frame frame = pool.acquire(node.id());
        frame.ioLock.writeLock().lock();
        try {
            writeToFrame(node, frame);
        } finally {
            frame.ioLock.writeLock().unlock();
            frame.unpin();
        }
        return node;
    }

    @Override
    public WriteHandle latchForWrite(long id) {
        Frame frame = pool.acquire(id); // pin held → frame cannot be repurposed → identity stable
        frame.treeLatch().lock();
        return new PinnedWriteHandle(frame);
    }

    @Override
    public long allocate() {
        return freeLog.acquireSafe(epochs.safeEpoch()).orElseGet(discStore::allocateRaw);
    }

    @Override
    public void retire(long pageId) {
        freeLog.retire(pageId, epochs.current());
        epochs.advance();
    }

    @Override
    public long enterOp() {
        return epochs.enter();
    }

    @Override
    public void exitOp(long ticket) {
        epochs.exit(ticket);
    }

    @Override
    public void updateRootId(long id) {
        discStore.updateRootId(id);
    }

    @Override
    public long rootId() {
        return discStore.rootId();
    }

    public long highWaterMark() {
        return discStore.highWaterMark();
    }

    /** Buffer-pool stats and frame-level access (monitoring, tests). */
    public BufferedPool pool() {
        return pool;
    }

    private static void writeToFrame(Node node, Frame frame) {
        ByteBuffer serialized = PageCodec.serialize(node);
        System.arraycopy(serialized.array(), 0, frame.bytes, 0, Store.PAGE_SIZE);
        frame.setDirty(true);
        frame.bumpUsage();
    }

    @Override
    public void close() throws IOException {
        pool.close(); // flush dirty frames first
        discStore.writeEpoch(epochs.current());
        discStore.close();
    }

    private static final class PinnedWriteHandle implements WriteHandle {
        private final Frame frame;
        private volatile boolean closed = false;

        PinnedWriteHandle(Frame frame) {
            this.frame = frame;
        }

        @Override
        public Node get() {
            frame.ioLock.readLock().lock();
            try {
                return PageCodec.deserialize(frame.pageId(), ByteBuffer.wrap(frame.bytes));
            } finally {
                frame.ioLock.readLock().unlock();
            }
        }

        @Override
        public void set(Node node) {
            frame.ioLock.writeLock().lock();
            try {
                writeToFrame(node, frame);
            } finally {
                frame.ioLock.writeLock().unlock();
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            frame.treeLatch().unlock();
            frame.unpin();
        }
    }
}
