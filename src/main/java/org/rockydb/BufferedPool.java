package org.rockydb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Concurrent, clock-evicted in-memory page cache — the sole {@link Store} implementation and the
 * persistence layer used by {@link BLinkTree}. It wraps a package-private {@link DiscStore} (raw
 * positional disc I/O); {@link DiscStore} performs no locking of its own, so every disc access is
 * serialized exclusively by this pool's per-frame {@code ioLock} (no double-locking).
 *
 * <h2>What is cached</h2>
 * Raw {@link Store#PAGE_SIZE} pages, not deserialized {@link Node} objects. {@link #readNode}
 * re-parses the cached page into a fresh node on every call and {@link #writeNode} serializes
 * into the cached page. This mirrors PostgreSQL (which caches fixed 8 KiB blocks) and, crucially,
 * preserves the B-link tree's existing concurrency contract: {@code LeafNode.copyWith} /
 * {@code BranchNode.copyWith} mutate their arrays in place on the upsert path while {@code get}
 * is lock-free, so caching shared {@code Node} objects would expose those mid-mutation arrays to
 * concurrent readers. Re-parsing per read is the price of correctness given that contract.
 *
 * <h2>Concurrency model</h2>
 * A fixed {@link Frame} array (the clock sweep's universe), a {@code pageId -> Frame} map for
 * lookups, and a {@link Clock} hand. Accessors <em>pin</em> the frame (a stacking refcount) so it
 * cannot be evicted while in use; the clock claims only unpinned frames whose usage count has been
 * driven to zero. Each frame's {@code ioLock} guards both its bytes <em>and</em> its identity: a
 * frame is repurposed only under the write side of {@code ioLock}, and accessors re-verify
 * {@code pageId} under their lock before relying on the bytes. The read/write lock's mutual
 * exclusion is what guarantees a reader never observes a page mid-repurpose.
 *
 * <p>Because every disc read (cache-miss load) and disc write (eviction flush, {@code close()})
 * happens under the corresponding frame's {@code ioLock} write side, the {@code ioLock} is the
 * sole guard against torn page I/O — {@link DiscStore} needs no lock of its own.
 *
 * <p>Dirty pages are flushed to {@link DiscStore} either when their frame is evicted or on
 * {@link #close()}. Root metadata ({@link #rootId}/{@link #updateRootId}/{@link #nodeIdGenerator})
 * bypasses the cache and delegates to the underlying {@link DiscStore}: page 0 is the reserved
 * root-id header and is never a tree node, so it is never cached.
 */
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
                    // Wrap (not duplicate) the frame's bytes: fresh buffer, independent position,
                    // shared array read-only for the duration of the parse under the read lock.
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
            // else: evicted/repurposed between acquire and lock — re-acquire and retry.
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

    /**
     * Pin the frame for {@code id} and acquire its tree write latch. The cache-miss load (and any
     * disc read it implies) happens in {@link #acquirePinned}, <em>before</em> the latch is taken, so
     * the returned handle performs no disc I/O while latched. The caller must {@link WriteHandle#close()}
     * it (in a {@code finally}).
     */
    @Override
    public WriteHandle latchForWrite(long id) {
        ensureOpen();
        while (true) {
            Frame frame = acquirePinned(id); // pins; loads on miss (disc I/O happens here, before the latch)
            // Re-verify the frame still caches id before latching. Between acquirePinned's map
            // lookup and its pin the frame can be claimed, evicted, and repurposed for a
            // different id (readNode/writeNode re-verify for the same reason); without this check
            // we would latch and mutate the wrong page. The check runs under the read side of
            // ioLock to exclude a concurrent repurpose already in flight, and the pin we hold
            // (pinCount >= 1) prevents any further repurpose — so once verified, the frame's
            // identity is stable for the operation.
            //
            // IMPORTANT: the ioLock read lock is RELEASED before the tree latch is taken. Taking
            // the tree latch while holding the ioLock read lock would invert the lock order
            // against get/set (which run with the tree latch held and then take ioLock:
            // treeLatch -> ioLock). latchForWrite holding ioLock -> treeLatch creates a classic
            // AB-BA deadlock between two writers on the same frame: one waits for the tree latch
            // under the read lock while the other (in set) waits for the write lock under the
            // tree latch. The pin alone keeps pageId stable from the check to the latch, so the
            // read lock is not needed across the latch acquisition.
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

    /**
     * A {@link WriteHandle} backed by a pinned, tree-latched frame. {@link #get}/{@link #set} take
     * the frame's {@code ioLock} only for the in-memory byte access; the tree latch is held across
     * the whole logical operation and released on {@link #close()} together with the pin.
     */
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

    /**
     * Acquire a frame pinned for {@code id}. Fast path: the page is already cached in the map.
     * Slow path: claim a clock victim, flush it if dirty, and load {@code id} from disk.
     *
     * <p>The returned frame is pinned; the caller MUST {@link Frame#unpin()} it (in a finally)
     * and must re-verify {@code pageId == id} under the frame's {@code ioLock} before relying on
     * its bytes — the page may have been repurposed between the map lookup and the lock.
     */
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

    /**
     * Install {@code victim} as the frame for {@code id}: drop its old mapping, flush the old page
     * if dirty, and load {@code id} from disk — all under the victim's {@code ioLock} write side.
     * The caller already holds the eviction claim (pinCount == 1); on success the claim is retained
     * for the caller, on failure the victim is left untouched and the caller releases the claim.
     *
     * @return {@code true} if the victim now caches {@code id}; {@code false} if another thread
     *         won the slot for {@code id} (victim unchanged).
     */
    private boolean installAndLoad(Frame victim, long id) {
        victim.ioLock.writeLock().lock();
        try {
            Frame existing = pageToFrame.putIfAbsent(id, victim);
            if (existing != null) {
                return false; // another frame already caches id; victim untouched
            }
            long oldId = victim.pageId();
            if (oldId != Frame.FREE) {
                // Flush the victim's previous page to disk BEFORE dropping its old mapping,
                // while the old mapping still resolves to this frame and we hold the write lock.
                // This closes the lost-update window: a concurrent loader of oldId is forced
                // through this frame's write lock (and observes the repurpose) or loads from
                // disk only after the flush — it can never read a stale pre-flush disk copy.
                if (victim.isDirty()) {
                    discStore.writeRawPage(oldId, ByteBuffer.wrap(victim.bytes));
                }
                // Conditional remove: only drop the old mapping if it still points at us.
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

    /**
     * Flush all dirty frames and close the underlying store. Not safe to call concurrently with
     * in-flight reads/writes — close the pool once accessors have quiesced.
     */
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

    // ----------------------------- statistics -----------------------------

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

    /** Number of frames currently holding a page. An approximate snapshot (no global lock). */
    public int getUsedFrames() {
        int count = 0;
        for (Frame frame : frames) {
            if (!frame.isFree()) {
                count++;
            }
        }
        return count;
    }

    /** Number of frames holding a dirty (unflushed) page. An approximate snapshot. */
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
