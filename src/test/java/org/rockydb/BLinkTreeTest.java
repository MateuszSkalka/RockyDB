package org.rockydb;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BLinkTreeTest {

    private static byte[] v(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class FakeStore implements Store {
        final Map<Long, byte[]> pages = new HashMap<>();
        private long nextId;
        private long rootId;
        private long count;
        int enters;
        int exits;

        static FakeStore withEmptyLeafRoot() {
            FakeStore s = new FakeStore();
            s.rootId = 1;
            s.nextId = 2;
            byte[] root = new byte[Store.PAGE_SIZE];
            LeafPage.formatEmpty(root, 1, Pages.NO_LINK, null);
            s.pages.put(s.rootId, root);
            return s;
        }

        byte[] page(long id) {
            return pages.get(id);
        }

        @Override
        public PageRef readPage(long id) {
            return readPage(id, true);
        }

        @Override
        public PageRef readPage(long id, boolean bumpUsage) {
            byte[] p = pages.get(id);
            if (p == null) {
                throw new IllegalArgumentException("no such page: " + id);
            }
            return new Ref(id, p);
        }

        @Override
        public PageWrite latchForWrite(long id) {
            byte[] p = pages.get(id);
            if (p == null) {
                throw new IllegalArgumentException("no such page: " + id);
            }
            return new Write(id, p);
        }

        @Override
        public PageWrite latchForWriteBlank(long id) {
            byte[] p = new byte[Store.PAGE_SIZE];
            pages.put(id, p);
            return new Write(id, p);
        }

        @Override
        public void updateRootId(long id) {
            rootId = id;
        }

        @Override
        public long rootId() {
            return rootId;
        }

        @Override
        public long elementCount() {
            return count;
        }

        @Override
        public void addToElementCount(long delta) {
            count += delta;
        }

        @Override
        public long allocate() {
            return nextId++;
        }

        @Override
        public void retire(long pageId) {
        }

        @Override
        public long enterOp() {
            enters++;
            return 0;
        }

        @Override
        public void exitOp(long ticket) {
            exits++;
        }

        private record Ref(long id, byte[] bytes) implements PageRef {
            @Override
            public void close() {}
        }

        private record Write(long id, byte[] bytes) implements PageWrite {
            @Override
            public void markDirty() {}

            @Override
            public void close() {}
        }
    }

    private static int fillUntilRootIsBranch(FakeStore store, BLinkTree tree, int children) {
        int i = 0;
        while (Pages.isLeaf(store.page(store.rootId()))
                || Pages.count(store.page(store.rootId())) != children) {
            tree.addValue(v("k%05d".formatted(i)), v("value-%05d".formatted(i)));
            i++;
            assertTrue(i < 100_000, "root never reached the wanted shape");
        }
        return i;
    }

    @Test
    void getAndInsertOnSingleLeafRoot() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);

        tree.addValue(v("a"), v("1"));
        tree.addValue(v("b"), v("2"));
        tree.addValue(v("a"), v("3"));

        assertArrayEquals(v("3"), tree.get(v("a")));
        assertArrayEquals(v("2"), tree.get(v("b")));
        assertNull(tree.get(v("c")));
        assertEquals(2, tree.count());
    }

    @Test
    void rootLeafSplitCreatesBranchRootWithTwoDownlinks() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);

        int n = fillUntilRootIsBranch(store, tree, 2);

        byte[] root = store.page(store.rootId());
        assertFalse(Pages.isLeaf(root));
        assertEquals(2, Pages.count(root));
        assertEquals(2, Pages.height(root));
        for (int i = 0; i < n; i++) {
            assertArrayEquals(v("value-%05d".formatted(i)), tree.get(v("k%05d".formatted(i))));
        }
        assertEquals(n, tree.count());
    }

    @Test
    void rightmostLeafSplitKeepsInfinityDownlinkInParent() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        fillUntilRootIsBranch(store, tree, 2);

        int i = 0;
        while (Pages.count(store.page(store.rootId())) != 3) {
            tree.addValue(v("z%05d".formatted(i)), v("w"));
            i++;
            assertTrue(i < 100_000);
        }

        byte[] root = store.page(store.rootId());
        assertEquals(3, Pages.count(root));
        assertNull(BranchPage.biggestKeyBytes(root));
        assertTrue((root[Pages.FLAGS] & Pages.FLAG_INF_LAST) != 0);
        for (int j = 0; j < i; j++) {
            assertArrayEquals(v("w"), tree.get(v("z%05d".formatted(j))));
        }
    }

    @Test
    void splitsCascadeToHeightThree() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        byte[] big = v("x".repeat(200));

        int n = 0;
        while (Pages.height(store.page(store.rootId())) < 3 && n < 100_000) {
            tree.addValue(v("k%06d".formatted(n)), big);
            n++;
        }

        assertEquals(3, Pages.height(store.page(store.rootId())));
        for (int i = 0; i < n; i++) {
            assertArrayEquals(big, tree.get(v("k%06d".formatted(i))));
        }
        assertEquals(n, tree.count());
    }

    @Test
    void deleteAbsentKeyChangesNothing() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        tree.addValue(v("a"), v("1"));

        tree.delete(v("missing"));

        assertArrayEquals(v("1"), tree.get(v("a")));
        assertEquals(1, tree.count());
    }

    @Test
    void deleteMergesRightLeafIntoLeftTombstonesItAndCollapsesRoot() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        int n = fillUntilRootIsBranch(store, tree, 2);

        byte[] root = store.page(store.rootId());
        long leftId = BranchPage.childAt(root, 0);
        long rightId = BranchPage.childAt(root, 1);
        byte[] left = store.page(leftId);
        byte[] right = store.page(rightId);

        byte[] leftFence = LeafPage.fenceBytes(left);
        List<String> leftKeys = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String k = "k%05d".formatted(i);
            if (java.util.Arrays.compareUnsigned(b(k), leftFence) <= 0) {
                leftKeys.add(k);
            }
        }
        int kept = Math.max(1, leftKeys.size() / 10);
        for (int i = kept; i < leftKeys.size(); i++) {
            tree.delete(v(leftKeys.get(i)));
        }

        assertTrue(Pages.isMarked(right), "right leaf should be tombstoned after merge");
        assertEquals(leftId, Pages.link(right), "tombstone routes to the survivor");
        assertTrue(Pages.isLeaf(store.page(store.rootId())), "root should collapse to the merged leaf");

        for (int i = 0; i < kept; i++) {
            assertArrayEquals(v("value-" + leftKeys.get(i).substring(1)), tree.get(v(leftKeys.get(i))));
        }
        for (int i = 0; i < n; i++) {
            String k = "k%05d".formatted(i);
            if (java.util.Arrays.compareUnsigned(b(k), leftFence) > 0) {
                assertArrayEquals(v("value-%05d".formatted(i)), tree.get(v(k)), "right-half key lost: " + k);
            }
        }
        assertEquals(kept + (n - leftKeys.size()), tree.count());
    }

    @Test
    void deleteDoesNotMergeWhenRightSiblingTooBig() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);

        long leftId = store.allocate();
        byte[] left = new byte[Store.PAGE_SIZE];
        store.pages.put(leftId, left);
        LeafPage.formatEmpty(left, 1, -1L, b("m"));
        LeafPage.put(left, b("a"), v("1"));
        LeafPage.put(left, b("b"), v("2"));
        int fillers = 0;
        while (Pages.liveBytes(left) < Pages.MAX_BODY * 26 / 100) {
            LeafPage.put(left, b("k%04d".formatted(fillers)), v("w%04d".formatted(fillers)));
            fillers++;
        }
        assertTrue(Pages.needsMerge(left), "left should be underfilled (< 30%)");

        long rightId = store.allocate();
        byte[] right = new byte[Store.PAGE_SIZE];
        store.pages.put(rightId, right);
        LeafPage.formatEmpty(right, 1, Pages.NO_LINK, null);
        int i = 0;
        while (Pages.liveBytes(right) < Pages.MAX_BODY * 8 / 10) {
            LeafPage.put(right, b("z%05d".formatted(i)), v("w%05d".formatted(i)));
            i++;
        }

        Pages.setLink(left, rightId);
        long rootId = store.allocate();
        byte[] root = new byte[Store.PAGE_SIZE];
        store.pages.put(rootId, root);
        BranchPage.formatEmpty(root, 2, Pages.NO_LINK);
        BranchPage.insert(root, b("m"), leftId);
        BranchPage.insert(root, null, rightId);
        store.updateRootId(rootId);
        store.addToElementCount(2 + fillers + i);

        tree.delete(v("a"));

        assertFalse(Pages.isMarked(right), "sibling too big: no merge may happen");
        assertArrayEquals(v("2"), tree.get(v("b")));
        for (int f = 0; f < fillers; f++) {
            assertArrayEquals(v("w%04d".formatted(f)), tree.get(v("k%04d".formatted(f))));
        }
        for (int j = 0; j < i; j++) {
            assertArrayEquals(v("w%05d".formatted(j)), tree.get(v("z%05d".formatted(j))));
        }
        assertEquals(1 + fillers + i, tree.count());
    }

    @Test
    void getRoutesAroundMergeTombstone() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);

        long leaf1Id = store.allocate();
        long leaf2Id = store.allocate();
        byte[] leaf2 = new byte[Store.PAGE_SIZE];
        store.pages.put(leaf2Id, leaf2);
        LeafPage.formatEmpty(leaf2, 1, Pages.NO_LINK, null);
        LeafPage.put(leaf2, b("a"), v("1"));
        LeafPage.put(leaf2, b("b"), v("2"));
        LeafPage.put(leaf2, b("m"), v("13"));
        LeafPage.put(leaf2, b("z"), v("26"));
        byte[] leaf1 = new byte[Store.PAGE_SIZE];
        store.pages.put(leaf1Id, leaf1);
        LeafPage.formatTombstone(leaf1, 1, leaf2Id);

        long rootId = store.allocate();
        byte[] root = new byte[Store.PAGE_SIZE];
        store.pages.put(rootId, root);
        BranchPage.formatEmpty(root, 2, Pages.NO_LINK);
        BranchPage.insert(root, b("m"), leaf1Id);
        BranchPage.insert(root, null, leaf2Id);
        store.updateRootId(rootId);

        assertArrayEquals(v("1"), tree.get(v("a")));
        assertArrayEquals(v("2"), tree.get(v("b")));
        assertArrayEquals(v("13"), tree.get(v("m")));
        assertArrayEquals(v("26"), tree.get(v("z")));
        assertNull(tree.get(v("c")));
    }

    @Test
    void entriesOnEmptyTreeIsEmptyAndReleasesEpoch() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);

        Iterator<BLinkTree.Entry> it = tree.entries();

        assertFalse(it.hasNext());
        assertEquals(store.enters, store.exits);
    }

    @Test
    void entriesScansEveryLeafInAscendingOrder() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        int n = 2_000;
        for (int i = 0; i < n; i++) {
            tree.addValue(v("k%06d".formatted(i)), v("v%06d".formatted(i)));
        }

        Iterator<BLinkTree.Entry> it = tree.entries();
        int seen = 0;
        String prev = null;
        while (it.hasNext()) {
            BLinkTree.Entry e = it.next();
            String k = new String(e.key(), StandardCharsets.UTF_8);
            if (prev != null) {
                assertTrue(prev.compareTo(k) < 0, "out of order: " + prev + " then " + k);
            }
            prev = k;
            assertEquals("v" + k.substring(1), new String(e.value(), StandardCharsets.UTF_8));
            seen++;
        }
        assertEquals(n, seen);
        assertEquals(store.enters, store.exits);
    }

    @Test
    void entriesHoldsEpochUntilExhausted() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        for (int i = 0; i < 100; i++) {
            tree.addValue(v("k%03d".formatted(i)), v("v"));
        }

        Iterator<BLinkTree.Entry> it = tree.entries();
        assertEquals(store.enters, store.exits + 1, "iterator must hold its epoch ticket");
        it.next();
        assertEquals(store.enters, store.exits + 1, "mid-iteration: still held");
        it.forEachRemaining(e -> {});
        assertEquals(store.enters, store.exits, "exhausted: released");
    }

    @Test
    void entriesToleratesSplitsAheadOfScanPosition() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);
        int n = 1_000;
        for (int i = 0; i < n; i++) {
            tree.addValue(v("k%06d".formatted(i)), v("v%06d".formatted(i)));
        }

        Iterator<BLinkTree.Entry> it = tree.entries();
        for (int i = 0; i < 10; i++) {
            it.next();
        }
        for (int i = n; i < n + 500; i++) {
            tree.addValue(v("k%06d".formatted(i)), v("v%06d".formatted(i)));
        }

        List<String> seen = new ArrayList<>();
        it.forEachRemaining(e -> seen.add(new String(e.key(), StandardCharsets.UTF_8)));
        assertEquals(n + 500 - 10, seen.size(), "stable entries missed or duplicated");
        for (int i = 1; i < seen.size(); i++) {
            assertTrue(seen.get(i - 1).compareTo(seen.get(i)) < 0, "duplicates or disorder");
        }
        assertEquals("k000010", seen.get(0));
    }

    @Test
    void countTracksInsertsReplacesDeletesAndSplits() {
        FakeStore store = FakeStore.withEmptyLeafRoot();
        BLinkTree tree = new BLinkTree(store);

        int n = 3_000;
        for (int i = 0; i < n; i++) {
            tree.addValue(v("k%06d".formatted(i)), v("v"));
        }
        assertEquals(n, tree.count());
        for (int i = 0; i < n; i += 2) {
            tree.addValue(v("k%06d".formatted(i)), v("replaced"));
        }
        assertEquals(n, tree.count(), "replaces must not move the count");
        for (int i = 0; i < n; i += 3) {
            tree.delete(v("k%06d".formatted(i)));
        }
        assertEquals(n - n / 3, tree.count());
        for (int i = 0; i < n; i++) {
            boolean deleted = i % 3 == 0;
            if (deleted) {
                assertNull(tree.get(v("k%06d".formatted(i))));
            } else {
                assertArrayEquals(i % 2 == 0 ? v("replaced") : v("v"), tree.get(v("k%06d".formatted(i))));
            }
        }
    }
}
