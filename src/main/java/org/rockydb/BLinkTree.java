package org.rockydb;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * B-Link tree (Lehman & Yao) over raw slotted pages. All node access runs directly on
 * page byte arrays via the static {@link LeafPage}/{@link BranchPage} ops — no node
 * materialization anywhere: descents binary-search the slot array, writers mutate a
 * shadow copy of the page under their latches ({@link PageWrite}), and even split
 * propagation copies raw key bytes.
 */
public class BLinkTree {
    private final Store store;
    private final RootRef rootRef;

    /** A single key/value pair returned by {@link #entries()}. */
    public record Entry(Value key, Value value) {}

    public BLinkTree(Store store) {
        this(store, new StoreBackedRootRef(store));
    }

    BLinkTree(Store store, RootRef rootRef) {
        this.store = store;
        this.rootRef = rootRef;
    }

    public Value get(Value key) {
        long ticket = store.enterOp();
        try {
            return getUnderEpoch(key.bytes());
        } finally {
            store.exitOp(ticket);
        }
    }

    /**
     * Exact number of key/value pairs in the tree. O(1): the count is a cached
     * counter maintained by {@link #addValue} and {@link #delete} (MyISAM-style —
     * viable because there is no MVCC, so a single count is well-defined). Reflects
     * ops completed so far; concurrently running ops may or may not be included.
     */
    public long count() {
        return rootRef.count();
    }

    /**
     * Full-table scan in ascending key order: descends to the leftmost leaf and follows
     * right links across the leaf level. The iterator is LAZY — it streams one leaf at a
     * time (a single 8KB snapshot copy per leaf), so memory stays O(page) and tables
     * larger than RAM can be scanned — and holds its epoch ticket for its whole lifetime,
     * releasing it when exhausted (or if a read fails). <b>Callers must consume it
     * fully</b>: an abandoned iterator leaks its epoch ticket and stalls FreeLog page
     * recycling (accepted risk for internal use). Scan reads skip the clock-usage bump,
     * so a large scan does not evict the hot working set from the buffer pool.
     * Concurrent splits/merges during the scan are tolerated: merged-away tombstones route
     * to their survivor and already-emitted keys are skipped, so entries present for the
     * whole scan are neither missed nor duplicated (weak consistency: keys inserted behind
     * or deleted ahead of the scan position may not appear).
     */
    public Iterator<Entry> entries() {
        long ticket = store.enterOp();
        try {
            return new EntryIterator(ticket, leftmostLeafId());
        } catch (RuntimeException e) {
            store.exitOp(ticket);
            throw e;
        }
    }

    private long leftmostLeafId() {
        long id = rootRef.get();
        while (true) {
            try (PageRef ref = store.readPage(id)) {
                byte[] p = ref.bytes();
                if (Pages.isMarked(p)) { // merge/root-collapse tombstone: link → survivor
                    id = Pages.link(p);
                } else if (Pages.isLeaf(p)) {
                    return id;
                } else {
                    id = BranchPage.childAt(p, 0);
                }
            }
        }
    }

    /**
     * Streams the leaf level one leaf snapshot (single array copy) at a time. All state
     * between calls is {@code currentId} + {@code lastKey} + the current snapshot, so
     * resuming is just "re-read the next page fresh" — splits that moved entries right are
     * followed via links, merges route around tombstones, and keys {@code <= lastKey} are
     * never re-emitted.
     */
    private final class EntryIterator implements Iterator<Entry> {
        private final long ticket;
        private long currentId;
        private Value lastKey;
        private byte[] snapshot = new byte[0];
        private int count;
        private int idx;
        private boolean done;

        EntryIterator(long ticket, long startId) {
            this.ticket = ticket;
            this.currentId = startId;
        }

        @Override
        public boolean hasNext() {
            if (done) {
                return false;
            }
            return idx < count || advance();
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Value key = LeafPage.keyAt(snapshot, idx);
            Entry entry = new Entry(key, LeafPage.valueAt(snapshot, idx));
            lastKey = key;
            idx++;
            return entry;
        }

        /** Moves to the next leaf with un-emitted keys; releases the epoch when the
         *  chain ends or a read fails. */
        private boolean advance() {
            try {
                while (currentId != -1) {
                    try (PageRef ref = store.readPage(currentId, false)) { // no clock-usage bump
                        byte[] p = ref.bytes();
                        if (Pages.isMarked(p)) { // merged away mid-scan: route to the survivor
                            currentId = Pages.link(p);
                            continue;
                        }
                        snapshot = Arrays.copyOf(p, Store.PAGE_SIZE);
                    }
                    int from = 0;
                    if (lastKey != null) {
                        int i = LeafPage.find(snapshot, lastKey.bytes());
                        from = i >= 0 ? i + 1 : -(i + 1); // skip keys already emitted
                    }
                    // Cross via this snapshot's link: its keys and link are consistent
                    // (copied under the read lock), anything that moved right is caught by
                    // tombstone routing + the lastKey skip at the target.
                    currentId = Pages.link(snapshot);
                    if (from < Pages.count(snapshot)) {
                        count = Pages.count(snapshot);
                        idx = from;
                        return true;
                    }
                }
                done = true;
                store.exitOp(ticket);
                return false;
            } catch (RuntimeException e) {
                done = true;
                store.exitOp(ticket); // don't leak the epoch on failure
                throw e;
            }
        }
    }

    private Value getUnderEpoch(byte[] key) {
        long id = rootRef.get();
        while (true) {
            try (PageRef ref = store.readPage(id)) {
                byte[] p = ref.bytes();
                if (Pages.isLeaf(p)) {
                    long next = LeafPage.nextNode(p, key);
                    if (next == Pages.NO_LINK) {
                        return LeafPage.getValue(p, key);
                    }
                    id = next;
                } else {
                    id = BranchPage.nextNode(p, key);
                }
            }
        }
    }

    public void addValue(Value key, Value value) {
        long ticket = store.enterOp();
        try {
            addValueUnderEpoch(key.bytes(), value.bytes());
        } finally {
            store.exitOp(ticket);
        }
    }

    /**
     * Descends from the root to the leaf responsible for {@code key}, latches it and
     * latch-couples rightward past any splits until the correct leaf is held. Branch
     * nodes followed by downlink (not right-link hops) are pushed onto {@code ancestors}
     * for later split propagation. Returns the latched leaf's handle.
     */
    private PageWrite latchLeafForKey(byte[] key, Deque<Long> ancestors) {
        long currentId = rootRef.get();
        while (true) {
            try (PageRef ref = store.readPage(currentId)) {
                byte[] p = ref.bytes();
                if (Pages.isLeaf(p)) {
                    break;
                }
                long next = BranchPage.nextNode(p, key);
                if (Pages.link(p) != next) { // downlink, not a right-link hop
                    ancestors.push(currentId);
                }
                currentId = next;
            }
        }

        PageWrite handle = store.latchForWrite(currentId);
        try {
            long next;
            while ((next = LeafPage.nextNode(handle.bytes(), key)) != Pages.NO_LINK) {
                PageWrite nextHandle = store.latchForWrite(next);
                handle.close();
                handle = nextHandle;
            }
            return handle;
        } catch (RuntimeException e) {
            handle.close();
            throw e;
        }
    }

    private void addValueUnderEpoch(byte[] key, byte[] value) {
        Deque<Long> ancestors = new ArrayDeque<>();
        PageWrite handle = latchLeafForKey(key, ancestors);
        try {
            LeafPage.PutResult put = LeafPage.put(handle.bytes(), key, value);
            if (put != LeafPage.PutResult.SPLIT_NEEDED) {
                handle.markDirty();
                if (put == LeafPage.PutResult.INSERTED) { // replace leaves the count unchanged
                    rootRef.addToCount(1);
                }
                return;
            }

            // The leaf is latched, so this existence check is exact (splitWithEntry embeds
            // both fresh inserts and grow-replaces; only brand-new keys grow the count).
            boolean isNewKey = LeafPage.find(handle.bytes(), key) < 0;

            // Split cascade: `handle` always latches the splitting (left) page. The first
            // split embeds the pending put; later splits embed the pending downlink change.
            boolean splittingLeaf = true;
            byte[] splitKey = key;     // leaf: put key;      branch: downlink fence to replace
            byte[] splitValue = value; // leaf: put value;    branch: promoted key replacing it
            long splitChildId = -1;    // branch only: child of the new right downlink
            while (true) {
                byte[] left = handle.bytes();
                long leftId = handle.id();
                int leftHeight = Pages.height(left);
                // old fence = parent's downlink key for left; capture before the split rewrites it
                byte[] oldFence = splittingLeaf ? LeafPage.fenceBytes(left) : BranchPage.biggestKeyBytes(left);
                long rightId = store.allocate();
                byte[] promoted;
                try (PageWrite right = store.latchForWriteBlank(rightId)) {
                    promoted = splittingLeaf
                            ? LeafPage.splitWithEntry(left, right.bytes(), rightId, oldFence, splitKey, splitValue)
                            : BranchPage.splitWithEntry(left, right.bytes(), rightId, splitKey, splitValue, splitChildId);
                    right.markDirty();
                }
                handle.markDirty();

                if (rootRef.get() == leftId) { // root split: height grows
                    createNewRoot(leftId, rightId, promoted, oldFence, leftHeight + 1);
                    break;
                }

                long parentId = !ancestors.isEmpty()
                        ? ancestors.pop()
                        : descendToLevel(promoted, leftHeight + 1);
                PageWrite parent = latchLiveNode(parentId, oldFence);
                boolean inserted;
                try {
                    inserted = BranchPage.insertSplit(parent.bytes(), oldFence, promoted, rightId);
                    if (inserted) {
                        parent.markDirty();
                    }
                } catch (RuntimeException e) {
                    parent.close();
                    throw e;
                }
                if (inserted) {
                    parent.close();
                    break;
                }
                // parent itself must split: it becomes the new left, embedding its own
                // pending downlink change in the halves
                handle.close();
                handle = parent;
                splittingLeaf = false;
                splitKey = oldFence;
                splitValue = promoted;
                splitChildId = rightId;
            }
            if (isNewKey) {
                rootRef.addToCount(1);
            }
        } finally {
            handle.close();
        }
    }

    public void delete(Value key) {
        long ticket = store.enterOp();
        try {
            deleteUnderEpoch(key.bytes());
        } finally {
            store.exitOp(ticket);
        }
    }

    private void deleteUnderEpoch(byte[] key) {
        Deque<Long> ancestors = new ArrayDeque<>();
        PageWrite handle = latchLeafForKey(key, ancestors);
        boolean merged = false;
        try {
            if (!LeafPage.remove(handle.bytes(), key)) {
                return; // absent: no mutation, no dirty marking
            }
            handle.markDirty();
            rootRef.addToCount(-1);

            while (Pages.link(handle.bytes()) != Pages.NO_LINK && Pages.needsMerge(handle.bytes())) {
                PageWrite ascended = tryAbsorbRight(handle, ancestors);
                if (ascended == null) {
                    break;
                }
                merged = true;
                handle = ascended;
            }
        } finally {
            handle.close();
        }
        // Only a merge (downlink removal) can leave the root with a single child, so skip
        // the extra pooled root read when this delete merged nothing. Must run after the
        // handle is closed: the topmost merged node may be the root itself, and latching
        // it again while a shadow for it is still open would let that stale shadow
        // overwrite the collapse tombstone at close.
        if (merged) {
            collapseRootIfSingleChild();
        }
    }

    /**
     * Merges the right sibling into the currently latched node, tombstones and retires the
     * right page, and removes its downlink from the parent. Returns the latched parent to
     * continue the upward cascade, or {@code null} when the merge cannot proceed (sibling
     * marked/too big, no parent candidate, downlinks no longer adjacent) — in that case all
     * handles except the caller's are released.
     */
    private PageWrite tryAbsorbRight(PageWrite handle, Deque<Long> ancestors) {
        byte[] current = handle.bytes();
        long rightId = Pages.link(current);

        PageWrite rightHandle = store.latchForWrite(rightId);
        byte[] right = rightHandle.bytes();
        boolean rightIsLeaf = Pages.isLeaf(right);
        boolean canMerge = !Pages.isMarked(right)
                && (rightIsLeaf ? LeafPage.canMerge(current, right) : BranchPage.canMerge(current, right));
        if (!canMerge || ancestors.isEmpty()) {
            rightHandle.close();
            return null;
        }

        byte[] rightFence = rightIsLeaf ? LeafPage.fenceBytes(right) : BranchPage.biggestKeyBytes(right);
        byte[] curFence = Pages.isLeaf(current) ? LeafPage.fenceBytes(current) : BranchPage.biggestKeyBytes(current);
        PageWrite parentHandle = latchLiveNode(ancestors.peek(), rightFence);
        byte[] parent = parentHandle.bytes();
        int rightIdx = BranchPage.find(parent, rightFence);
        int curIdx = BranchPage.find(parent, curFence);
        if (rightIdx < 0 || curIdx < 0 || rightIdx != curIdx + 1) {
            parentHandle.close();
            rightHandle.close();
            return null;
        }

        if (rightIsLeaf) {
            LeafPage.mergeFrom(current, right);
            LeafPage.formatTombstone(right, Pages.height(right), handle.id());
        } else {
            BranchPage.mergeFrom(current, right);
            BranchPage.formatTombstone(right, Pages.height(right), handle.id());
        }
        handle.markDirty();
        rightHandle.markDirty();
        long rightPageId = rightHandle.id();
        rightHandle.close();
        store.retire(rightPageId);

        if (!BranchPage.removeDownlink(parent, rightFence)) { // merge stays applied either way
            parentHandle.close();
            return null;
        }
        parentHandle.markDirty();
        ancestors.pop();
        handle.close();
        return parentHandle;
    }

    /**
     * Latches {@code startId} and latch-couples rightward while the node is a tombstone or
     * its fence is below {@code guideKey} ({@code null} = +∞), returning the live node.
     */
    private PageWrite latchLiveNode(Long startId, byte[] guideKey) {
        PageWrite h = store.latchForWrite(startId);
        while (shouldGoRight(h.bytes(), guideKey)) {
            PageWrite next = store.latchForWrite(Pages.link(h.bytes()));
            h.close();
            h = next;
        }
        return h;
    }

    private static boolean shouldGoRight(byte[] p, byte[] key) {
        return Pages.isLeaf(p) ? LeafPage.shouldGoRight(p, key) : BranchPage.shouldGoRight(p, key);
    }

    private void collapseRootIfSingleChild() {
        while (true) {
            long rootId = rootRef.get();
            try (PageRef ref = store.readPage(rootId)) {
                byte[] root = ref.bytes();
                if (Pages.isLeaf(root) || Pages.isMarked(root) || Pages.count(root) != 1) {
                    return;
                }
            }

            PageWrite h = store.latchForWrite(rootId);
            try {
                if (rootRef.get() != rootId) {
                    continue;
                }
                byte[] root = h.bytes();
                if (Pages.isMarked(root) || Pages.count(root) != 1) {
                    return;
                }
                long child = BranchPage.childAt(root, 0);
                BranchPage.formatTombstone(root, Pages.height(root), child);
                h.markDirty();
                store.retire(rootId);
                rootRef.set(child);
            } finally {
                h.close();
            }
        }
    }

    /** {@code rightFence == null} encodes the +∞ downlink key of a rightmost child. */
    private void createNewRoot(long leftId, long rightId, byte[] promoted, byte[] rightFence, int height) {
        long newRootId = store.allocate();
        try (PageWrite root = store.latchForWriteBlank(newRootId)) {
            BranchPage.formatEmpty(root.bytes(), height, Pages.NO_LINK);
            BranchPage.insert(root.bytes(), promoted, leftId);
            BranchPage.insert(root.bytes(), rightFence, rightId);
            root.markDirty();
        }
        rootRef.set(newRootId);
    }

    private long descendToLevel(byte[] key, int targetHeight) {
        long currentId = rootRef.get();
        while (true) {
            try (PageRef ref = store.readPage(currentId)) {
                byte[] p = ref.bytes();
                if (Pages.height(p) <= targetHeight) {
                    return currentId;
                }
                currentId = BranchPage.nextNode(p, key);
            }
        }
    }

    private static final class StoreBackedRootRef implements RootRef {
        private final Store store;

        StoreBackedRootRef(Store store) {
            this.store = store;
        }

        @Override
        public long get() {
            return store.rootId();
        }

        @Override
        public void set(long rootId) {
            store.updateRootId(rootId);
        }

        @Override
        public long count() {
            return store.elementCount();
        }

        @Override
        public void addToCount(long delta) {
            store.addToElementCount(delta);
        }
    }
}
