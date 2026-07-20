package org.rockydb;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class BranchPageTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] page() {
        return new byte[Store.PAGE_SIZE];
    }

    private static TreeMap<String, Long> entriesOf(byte[] p) {
        TreeMap<String, Long> out = new TreeMap<>();
        for (int i = 0; i < Pages.count(p); i++) {
            out.put(new String(BranchPage.keyBytesAt(p, i), StandardCharsets.UTF_8), BranchPage.childAt(p, i));
        }
        return out;
    }

    private static byte[] branchWith(Object... keyThenChild) {
        byte[] p = page();
        BranchPage.formatEmpty(p, 2, Pages.NO_LINK);
        for (int i = 0; i < keyThenChild.length; i += 2) {
            assertTrue(BranchPage.insert(p, b((String) keyThenChild[i]), (long) keyThenChild[i + 1]));
        }
        return p;
    }

    @Test
    void insertKeepsSortedOrderAndFindLocatesKeys() {
        byte[] p = branchWith("m", 2L, "a", 1L, "z", 3L);

        assertEquals(3, Pages.count(p));
        assertEquals(Map.of("a", 1L, "m", 2L, "z", 3L), entriesOf(p));
        assertEquals(2L, BranchPage.childAt(p, BranchPage.find(p, b("m"))));
        assertTrue(BranchPage.find(p, b("q")) < 0);
        assertArrayEquals(b("z"), BranchPage.biggestKeyBytes(p));
    }

    @Test
    void nextNodeDescendsToFirstFenceAtOrAboveKey() {
        byte[] p = branchWith("m", 10L, "t", 20L, "z", 30L);

        assertEquals(10L, BranchPage.nextNode(p, b("a")));
        assertEquals(10L, BranchPage.nextNode(p, b("m")));
        assertEquals(20L, BranchPage.nextNode(p, b("n")));
        assertEquals(30L, BranchPage.nextNode(p, b("z")));
    }

    @Test
    void nextNodePastLastFenceGoesRightOrToLastChild() {
        byte[] withLink = branchWith("m", 10L, "z", 20L);
        Pages.setLink(withLink, 77L);
        assertEquals(77L, BranchPage.nextNode(withLink, b("zz")));

        byte[] rightmost = branchWith("m", 10L, "z", 20L);
        assertEquals(20L, BranchPage.nextNode(rightmost, b("zz")));
    }

    @Test
    void nextNodeOnMarkedBranchRoutesToSurvivor() {
        byte[] p = page();
        BranchPage.formatTombstone(p, 4, 55L);

        assertTrue(Pages.isMarked(p));
        assertFalse(Pages.isLeaf(p));
        assertEquals(55L, BranchPage.nextNode(p, b("anything")));
        assertTrue(BranchPage.shouldGoRight(p, b("anything")));
    }

    @Test
    void shouldGoRightComparesAgainstLastFence() {
        byte[] p = branchWith("m", 10L, "z", 20L);
        Pages.setLink(p, 1L);

        assertFalse(BranchPage.shouldGoRight(p, b("z")));
        assertTrue(BranchPage.shouldGoRight(p, b("zz")));
    }

    @Test
    void insertSplitReplacesDownlinkAndAddsRightSibling() {
        byte[] p = branchWith("m", 10L, "z", 20L);

        assertTrue(BranchPage.insertSplit(p, b("m"), b("g"), 99L));

        assertEquals(Map.of("g", 10L, "m", 99L, "z", 20L), entriesOf(p));
    }

    @Test
    void insertSplitThrowsWhenOldFenceMissing() {
        byte[] p = branchWith("m", 10L);
        assertThrows(IllegalStateException.class, () -> BranchPage.insertSplit(p, b("nope"), b("x"), 1L));
    }

    @Test
    void insertSplitReturnsFalseWhenFullAndLeavesPageUnmodified() {
        byte[] p = page();
        BranchPage.formatEmpty(p, 2, Pages.NO_LINK);
        TreeMap<String, Long> expected = new TreeMap<>();
        int i = 0;
        while (true) {
            String k = "k%05d".formatted(i++);
            if (!BranchPage.insert(p, b(k), i)) {
                break;
            }
            expected.put(k, (long) i);
        }
        String someFence = expected.firstKey();
        assertFalse(BranchPage.insertSplit(p, b(someFence), b("aa-new"), 999L));
        assertEquals(expected, entriesOf(p));
    }

    @Test
    void splitWithEntryEmbedsPendingDownlinksAndBalances() {
        byte[] left = page();
        BranchPage.formatEmpty(left, 3, 77L);
        int i = 0;
        while (BranchPage.insert(left, b("k%05d".formatted(i)), 1000L + i)) {
            i++;
        }
        int fullCount = i;
        byte[] oldBiggest = BranchPage.biggestKeyBytes(left);

        byte[] right = page();
        byte[] promoted = BranchPage.splitWithEntry(left, right, 99L, b("k00010"), b("k00010a"), 55L);

        TreeMap<String, Long> all = entriesOf(left);
        all.putAll(entriesOf(right));
        assertEquals(fullCount + 1, all.size());
        assertEquals(1000L + 10, all.get("k00010a"));
        assertEquals(55L, all.get("k00010"));
        assertEquals(99L, Pages.link(left));
        assertEquals(77L, Pages.link(right));
        assertArrayEquals(promoted, BranchPage.biggestKeyBytes(left));
        assertArrayEquals(oldBiggest, BranchPage.biggestKeyBytes(right));
        assertTrue(Math.abs(Pages.count(left) - Pages.count(right)) <= 1);
        String leftMax = new String(BranchPage.biggestKeyBytes(left), StandardCharsets.UTF_8);
        String rightMin = new String(BranchPage.keyBytesAt(right, 0), StandardCharsets.UTF_8);
        assertTrue(leftMax.compareTo(rightMin) < 0);
    }

    @Test
    void splitWithEntryThrowsWhenOldFenceMissing() {
        byte[] left = branchWith("m", 1L);
        byte[] right = page();
        assertThrows(IllegalStateException.class,
                () -> BranchPage.splitWithEntry(left, right, 2L, b("nope"), b("x"), 3L));
    }

    @Test
    void removeDownlinkRemovesRightEntryAndKeepsLeftPointer() {
        byte[] p = branchWith("b", 10L, "m", 20L, "z", 30L);

        assertTrue(BranchPage.removeDownlink(p, b("m")));

        assertEquals(Map.of("m", 10L, "z", 30L), entriesOf(p));
    }

    @Test
    void removeDownlinkAtFirstSlot() {
        byte[] p = branchWith("b", 10L, "m", 20L);

        assertTrue(BranchPage.removeDownlink(p, b("b")));
        assertEquals(Map.of("m", 20L), entriesOf(p));
    }

    @Test
    void removeDownlinkRefusesToEmptyBranch() {
        byte[] p = branchWith("b", 10L);
        assertFalse(BranchPage.removeDownlink(p, b("b")));
        assertEquals(Map.of("b", 10L), entriesOf(p));
    }

    @Test
    void mergeAbsorbsRightBranchAndLink() {
        byte[] left = branchWith("b", 10L, "m", 20L);
        Pages.setLink(left, 8L);
        byte[] right = branchWith("t", 30L, "z", 40L);
        Pages.setLink(right, 9L);

        assertTrue(BranchPage.canMerge(left, right));
        BranchPage.mergeFrom(left, right);

        assertEquals(Map.of("b", 10L, "m", 20L, "t", 30L, "z", 40L), entriesOf(left));
        assertEquals(9L, Pages.link(left));
        assertArrayEquals(b("z"), BranchPage.biggestKeyBytes(left));
    }

    @Test
    void fuzzRandomInsertsMatchTreeMap() {
        Random rnd = new Random(777);
        byte[] p = page();
        BranchPage.formatEmpty(p, 2, Pages.NO_LINK);
        TreeMap<Long, Long> expected = new TreeMap<>();

        for (int round = 0; round < 30_000; round++) {
            long k = rnd.nextInt(500);
            byte[] kb = {(byte) (k >>> 8), (byte) k};
            long child = rnd.nextLong();
            if (!BranchPage.insert(p, kb, child)) {
                break;
            }
            expected.put(k, child);
        }

        assertEquals(expected.size(), Pages.count(p));
        long prev = -1;
        for (int i = 0; i < Pages.count(p); i++) {
            byte[] kb = BranchPage.keyBytesAt(p, i);
            long k = ((kb[0] & 0xFF) << 8) | (kb[1] & 0xFF);
            assertTrue(k > prev, "slots must be strictly ordered");
            prev = k;
            assertEquals(expected.get(k), BranchPage.childAt(p, i));
        }
    }
}
