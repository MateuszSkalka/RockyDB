package org.rockydb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class NodeStore implements Store, Closeable {

    private final DiscStore discStore;
    private final BufferedPool pool;
    private final EpochManager epochs;
    private final FreeLog freeLog;
    private final AtomicLong elementCount;

    public NodeStore(File dbFile, int numFrames) throws IOException {
        this.discStore = new DiscStore(dbFile);
        this.pool = new BufferedPool(discStore, numFrames);
        if (discStore.rootId() == -1) {
            bootstrap();
        }
        this.epochs = new EpochManager(discStore.loadEpoch() + 1);
        this.freeLog = new FreeLog(
                discStore, epochs, discStore.freeLogHeadPacked(), discStore.freeLogTailPacked());
        this.elementCount = new AtomicLong(discStore.loadCount());
    }

    private void bootstrap() {
        long dataRootId = discStore.allocateRaw();
        byte[] root = new byte[Store.PAGE_SIZE];
        LeafPage.formatEmpty(root, 1, Pages.NO_LINK, null);
        discStore.writeRawPage(dataRootId, ByteBuffer.wrap(root));
        discStore.updateRootId(dataRootId);

        long freePageId = discStore.allocateRaw();
        discStore.writeRawPage(freePageId, FreeLog.emptyPage());
        long packed = FreeLog.pack(freePageId, 0);
        discStore.writeFreeLogBootstrap(packed, packed);
    }

    @Override
    public PageRef readPage(long id) {
        return readPage(id, true);
    }

    @Override
    public PageRef readPage(long id, boolean bumpUsage) {
        Frame frame = pool.acquire(id);
        frame.ioLock.readLock().lock();
        if (bumpUsage) {
            frame.bumpUsage();
        }
        return new PinnedPageRef(frame);
    }

    @Override
    public PageWrite latchForWrite(long id) {
        return latch(pool.acquire(id));
    }

    @Override
    public PageWrite latchForWriteBlank(long id) {
        return latch(pool.acquireBlank(id));
    }

    private PageWrite latch(Frame frame) {
        // Pin keeps the frame un-repurposable; the writer mutates a shadow so readers never block.
        frame.treeLatch().lock();
        frame.ioLock.readLock().lock();
        byte[] shadow;
        try {
            shadow = Arrays.copyOf(frame.bytes, Store.PAGE_SIZE);
        } finally {
            frame.ioLock.readLock().unlock();
        }
        return new PinnedPageWrite(frame, shadow);
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

    @Override
    public long elementCount() {
        return elementCount.get();
    }

    @Override
    public void addToElementCount(long delta) {
        elementCount.addAndGet(delta);
    }

    public long highWaterMark() {
        return discStore.highWaterMark();
    }

    public BufferedPool pool() {
        return pool;
    }

    @Override
    public void close() throws IOException {
        freeLog.flush();
        pool.close();
        discStore.writeEpoch(epochs.current());
        discStore.writeCount(elementCount.get());
        discStore.close();
    }

    private static final class PinnedPageRef implements PageRef {
        private final Frame frame;
        private boolean closed = false;

        PinnedPageRef(Frame frame) {
            this.frame = frame;
        }

        @Override
        public long id() {
            return frame.pageId();
        }

        @Override
        public byte[] bytes() {
            return frame.bytes;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            frame.ioLock.readLock().unlock();
            frame.unpin();
        }
    }

    private static final class PinnedPageWrite implements PageWrite {
        private final Frame frame;
        private final byte[] shadow;
        private boolean dirty = false;
        private boolean closed = false;

        PinnedPageWrite(Frame frame, byte[] shadow) {
            this.frame = frame;
            this.shadow = shadow;
        }

        @Override
        public long id() {
            return frame.pageId();
        }

        @Override
        public byte[] bytes() {
            return shadow;
        }

        @Override
        public void markDirty() {
            frame.setDirty(true);
            frame.bumpUsage();
            dirty = true;
            // Publish immediately: merge tombstones the right sibling while the left page is
            // still latched, so a reader routing around the tombstone must see the absorbed keys.
            publish();
        }

        private void publish() {
            frame.ioLock.writeLock().lock();
            try {
                System.arraycopy(shadow, 0, frame.bytes, 0, Store.PAGE_SIZE);
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
            if (dirty) {
                publish();
            }
            frame.treeLatch().unlock();
            frame.unpin();
        }
    }
}
