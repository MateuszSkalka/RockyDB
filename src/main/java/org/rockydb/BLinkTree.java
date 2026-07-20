package org.rockydb;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BLinkTree {
    private final Store store;
    private final RootRef rootRef;

    public record Entry(byte[] key, byte[] value) {}

    public BLinkTree(Store store) {
        this(store, new StoreBackedRootRef(store));
    }

    BLinkTree(Store store, RootRef rootRef) {
        this.store = store;
        this.rootRef = rootRef;
    }

    public byte[] get(byte[] key) {
        long ticket = store.enterOp();
        try {
            return getUnderEpoch(key);
        } finally {
            store.exitOp(ticket);
        }
    }

    public long count() {
        return rootRef.count();
    }

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
                if (Pages.isMarked(p)) {
                    id = Pages.link(p);
                } else if (Pages.isLeaf(p)) {
                    return id;
                } else {
                    id = BranchPage.childAt(p, 0);
                }
            }
        }
    }

    private final class EntryIterator implements Iterator<Entry> {
        private final long ticket;
        private long currentId;
        private byte[] lastKey;
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
            byte[] key = LeafPage.keyAt(snapshot, idx);
            Entry entry = new Entry(key, LeafPage.valueAt(snapshot, idx));
            lastKey = key;
            idx++;
            return entry;
        }

        private boolean advance() {
            try {
                while (currentId != -1) {
                    try (PageRef ref = store.readPage(currentId, false)) {
                        byte[] p = ref.bytes();
                        if (Pages.isMarked(p)) {
                            currentId = Pages.link(p);
                            continue;
                        }
                        snapshot = Arrays.copyOf(p, Store.PAGE_SIZE);
                    }
                    int from = 0;
                    if (lastKey != null) {
                        int i = LeafPage.find(snapshot, lastKey);
                        from = i >= 0 ? i + 1 : -(i + 1);
                    }
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
                store.exitOp(ticket);
                throw e;
            }
        }
    }

    private byte[] getUnderEpoch(byte[] key) {
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

    public void addValue(byte[] key, byte[] value) {
        long ticket = store.enterOp();
        try {
            addValueUnderEpoch(key, value);
        } finally {
            store.exitOp(ticket);
        }
    }

    private PageWrite latchLeafForKey(byte[] key, Deque<Long> ancestors) {
        long currentId = rootRef.get();
        while (true) {
            try (PageRef ref = store.readPage(currentId)) {
                byte[] p = ref.bytes();
                if (Pages.isLeaf(p)) {
                    break;
                }
                long next = BranchPage.nextNode(p, key);
                if (Pages.link(p) != next) {
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
                if (put == LeafPage.PutResult.INSERTED) {
                    rootRef.addToCount(1);
                }
                return;
            }

            // exact: the leaf is latched, so only brand-new keys grow the count
            boolean isNewKey = LeafPage.find(handle.bytes(), key) < 0;

            boolean splittingLeaf = true;
            byte[] splitKey = key;
            byte[] splitValue = value;
            long splitChildId = -1;
            while (true) {
                byte[] left = handle.bytes();
                long leftId = handle.id();
                int leftHeight = Pages.height(left);
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

                if (rootRef.get() == leftId) {
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

    public void delete(byte[] key) {
        long ticket = store.enterOp();
        try {
            deleteUnderEpoch(key);
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
                return;
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
        // must run after the handle is closed: a stale shadow for the topmost merged
        // node (possibly the root) would overwrite the collapse tombstone at close
        if (merged) {
            collapseRootIfSingleChild();
        }
    }

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

        if (!BranchPage.removeDownlink(parent, rightFence)) {
            parentHandle.close();
            return null;
        }
        parentHandle.markDirty();
        ancestors.pop();
        handle.close();
        return parentHandle;
    }

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
