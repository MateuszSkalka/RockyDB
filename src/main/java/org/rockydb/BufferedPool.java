package org.rockydb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class BufferedPool implements Store, Closeable {

    private final DiscStore discStore;
    private final Frame[] frames;
    private final Clock clock;
    private final ConcurrentMap<Long, Frame> pageToFrame = new ConcurrentHashMap<>();

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();

    private volatile boolean closed = false;

    public BufferedPool(File dbFile, int numFrames) throws IOException {
        if (numFrames <= 0) {
            throw new IllegalArgumentException("numFrames must be > 0");
        }
        this.discStore = new DiscStore(dbFile);
        this.frames = new Frame[numFrames];
        for (int i = 0; i < numFrames; i++) {
            this.frames[i] = new Frame(i);
        }
        this.clock = new Clock(this.frames);
    }

    @Override
    public Node readNode(long id) {
        ensureOpen();
        while (true) {
            Frame frame = acquirePinned(id);
            Node result = null;
            boolean matched = false;
            frame.ioLock.readLock().lock();
            try {
                if (frame.pageId() == id) {
                    frame.bumpUsage();
                    hits.increment();
                    result = PageCodec.deserialize(id, ByteBuffer.wrap(frame.bytes));
                    matched = true;
                }
            } finally {
                frame.ioLock.readLock().unlock();
            }
            frame.unpin();
            if (matched) {
                return result;
            }
        }
    }

    @Override
    public Node writeNode(Node node) {
        ensureOpen();
        long id = node.id();
        while (true) {
            Frame frame = acquirePinned(id);
            boolean matched = false;
            frame.ioLock.writeLock().lock();
            try {
                if (frame.pageId() == id) {
                    ByteBuffer serialized = PageCodec.serialize(node);
                    System.arraycopy(serialized.array(), 0, frame.bytes, 0, Store.PAGE_SIZE);
                    frame.setDirty(true);
                    frame.bumpUsage();
                    matched = true;
                }
            } finally {
                frame.ioLock.writeLock().unlock();
            }
            frame.unpin();
            if (matched) {
                return node;
            }
        }
    }

    @Override
    public WriteHandle latchForWrite(long id) {
        ensureOpen();
        while (true) {
            Frame frame = acquirePinned(id);
            frame.ioLock.readLock().lock();
            boolean matches;
            try {
                matches = frame.pageId() == id;
            } finally {
                frame.ioLock.readLock().unlock();
            }
            if (matches) {
                frame.treeLatch().lock(); // pin held → frame cannot be repurposed → identity stable
                return new PinnedWriteHandle(frame);
            }
            frame.unpin(); // repurposed between lookup and pin; release and retry
        }
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
                ByteBuffer serialized = PageCodec.serialize(node);
                System.arraycopy(serialized.array(), 0, frame.bytes, 0, Store.PAGE_SIZE);
                frame.setDirty(true);
                frame.bumpUsage();
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

    @Override
    public Supplier<Long> nodeIdGenerator() {
        return discStore.nodeIdGenerator();
    }

    @Override
    public void updateRootId(long id) {
        discStore.updateRootId(id);
    }

    @Override
    public long rootId() {
        return discStore.rootId();
    }

    private Frame acquirePinned(long id) {
        while (true) {
            Frame cached = pageToFrame.get(id);
            if (cached != null) {
                cached.pin();
                return cached;
            }
            Frame victim = clock.findVictim(); // exclusively claimed (pinCount == 1)
            if (installAndLoad(victim, id)) {
                return victim; // pinned, pageId == id, content loaded
            }
            victim.unpin(); // lost the race for id; release the claim and retry
        }
    }

    private boolean installAndLoad(Frame victim, long id) {
        victim.ioLock.writeLock().lock();
        try {
            Frame existing = pageToFrame.putIfAbsent(id, victim);
            if (existing != null) {
                return false;
            }
            long oldId = victim.pageId();
            if (oldId != Frame.FREE) {

                if (victim.isDirty()) {
                    discStore.writeRawPage(oldId, ByteBuffer.wrap(victim.bytes));
                }
                pageToFrame.remove(oldId, victim);
                evictions.increment();
            }
            ByteBuffer page = discStore.readRawPage(id);
            System.arraycopy(page.array(), 0, victim.bytes, 0, Store.PAGE_SIZE);
            victim.setDirty(false);
            victim.setPageId(id);
            misses.increment();
            return true;
        } finally {
            victim.ioLock.writeLock().unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("BufferedPool is closed");
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        for (Frame frame : frames) {
            frame.ioLock.writeLock().lock();
            try {
                if (!frame.isFree() && frame.isDirty()) {
                    discStore.writeRawPage(frame.pageId(), ByteBuffer.wrap(frame.bytes));
                    frame.setDirty(false);
                }
            } finally {
                frame.ioLock.writeLock().unlock();
            }
        }
        try {
            discStore.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close underlying store", e);
        }
    }


    public int getBufferHits() {
        return (int) hits.sum();
    }

    public int getBufferMisses() {
        return (int) misses.sum();
    }

    public int getEvictions() {
        return (int) evictions.sum();
    }

    public int getNumFrames() {
        return frames.length;
    }

    public int getUsedFrames() {
        int count = 0;
        for (Frame frame : frames) {
            if (!frame.isFree()) {
                count++;
            }
        }
        return count;
    }

    public int getDirtyFrames() {
        int count = 0;
        for (Frame frame : frames) {
            if (!frame.isFree() && frame.isDirty()) {
                count++;
            }
        }
        return count;
    }

    public double getHitRatio() {
        long h = hits.sum();
        long m = misses.sum();
        long total = h + m;
        return total == 0 ? 0.0 : (double) h / (double) total;
    }
}
