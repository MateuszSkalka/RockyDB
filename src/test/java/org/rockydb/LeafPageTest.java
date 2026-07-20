package org.rockydb;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class LeafPageTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] page() {
        return new byte[Store.PAGE_SIZE];
    }

    private static TreeMap<String, String> entriesOf(byte[] p) {
        TreeMap<String, String> out = new TreeMap<>();
        for (int i = 0; i < Pages.count(p); i++) {
            out.put(new String(LeafPage.keyAt(p, i), StandardCharsets.UTF_8),
                    new String(LeafPage.valueAt(p, i), StandardCharsets.UTF_8));
        }
        return out;
    }

    private static TreeMap<String, byte[]> entriesOfBytes(byte[] p) {
        TreeMap<String, byte[]> out = new TreeMap<>();
        for (int i = 0; i < Pages.count(p); i++) {
            out.put(new String(LeafPage.keyBytesAt(p, i), StandardCharsets.UTF_8), LeafPage.valueAt(p, i));
        }
        return out;
    }

    private static byte[] leafWith(String... kv) {
        byte[] p = page();
        LeafPage.formatEmpty(p, 1, Pages.NO_LINK, null);
        for (int i = 0; i < kv.length; i += 2) {
            assertEquals(LeafPage.PutResult.INSERTED, LeafPage.put(p, b(kv[i]), b(kv[i + 1])));
        }
        return p;
    }

    @Test
    void putInsertsInSortedOrderAndGetValueFindsThem() {
        byte[] p = leafWith("c", "3", "a", "1", "b", "2");

        assertEquals(3, Pages.count(p));
        assertArrayEquals(b("1"), LeafPage.getValue(p, b("a")));
        assertArrayEquals(b("2"), LeafPage.getValue(p, b("b")));
        assertArrayEquals(b("3"), LeafPage.getValue(p, b("c")));
        assertNull(LeafPage.getValue(p, b("d")));
        assertEquals(Map.of("a", "1", "b", "2", "c", "3"), entriesOf(p));
    }

    @Test
    void putReplaceSameSizeValueOverwritesInPlace() {
        byte[] p = leafWith("a", "111");
        int deadBefore = Pages.deadBytes(p);

        assertEquals(LeafPage.PutResult.REPLACED, LeafPage.put(p, b("a"), b("222")));

        assertArrayEquals(b("222"), LeafPage.getValue(p, b("a")));
        assertEquals(1, Pages.count(p));
        assertEquals(deadBefore, Pages.deadBytes(p));
    }

    @Test
    void putReplaceWithSmallerValueReclaimsTheDifference() {
        byte[] p = leafWith("a", "long-value");
        int deadBefore = Pages.deadBytes(p);

        assertEquals(LeafPage.PutResult.REPLACED, LeafPage.put(p, b("a"), b("s")));

        assertArrayEquals(b("s"), LeafPage.getValue(p, b("a")));
        assertEquals(deadBefore + "long-value".length() - 1, Pages.deadBytes(p));
    }

    @Test
    void putReplaceWithLargerValueRelocatesTheTuple() {
        byte[] p = leafWith("a", "s", "b", "2", "c", "3");

        assertEquals(LeafPage.PutResult.REPLACED, LeafPage.put(p, b("a"), b("much-longer-value")));

        assertArrayEquals(b("much-longer-value"), LeafPage.getValue(p, b("a")));
        assertEquals(Map.of("a", "much-longer-value", "b", "2", "c", "3"), entriesOf(p));
    }

    @Test
    void putReportsSplitNeededAndLeavesPageLogicallyUnmodified() {
        byte[] p = page();
        LeafPage.formatEmpty(p, 1, Pages.NO_LINK, null);
        TreeMap<String, String> expected = new TreeMap<>();
        int i = 0;
        while (true) {
            String k = "k%05d".formatted(i);
            String v = "v%05d".formatted(i);
            if (LeafPage.put(p, b(k), b(v)) == LeafPage.PutResult.SPLIT_NEEDED) {
                break;
            }
            expected.put(k, v);
            i++;
        }
        byte[] bigValue = new byte[100];
        assertEquals(LeafPage.PutResult.SPLIT_NEEDED, LeafPage.put(p, b("zzzzz"), bigValue));
        assertEquals(expected, entriesOf(p));

        assertEquals(LeafPage.PutResult.SPLIT_NEEDED, LeafPage.put(p, b("k00000"), bigValue));
        assertEquals(expected, entriesOf(p));
    }

    @Test
    void removeDeletesKeyAndKeepsOrder() {
        byte[] p = leafWith("a", "1", "b", "2", "c", "3", "d", "4");

        assertTrue(LeafPage.remove(p, b("b")));
        assertTrue(LeafPage.remove(p, b("d")));
        assertFalse(LeafPage.remove(p, b("zz")));
        assertFalse(LeafPage.remove(p, b("0")));

        assertEquals(Map.of("a", "1", "c", "3"), entriesOf(p));
    }

    @Test
    void compactionReclaimsFragmentedSpace() {
        byte[] p = page();
        LeafPage.formatEmpty(p, 1, Pages.NO_LINK, null);
        TreeMap<String, String> expected = new TreeMap<>();
        int i = 0;
        while (LeafPage.put(p, b("k%05d".formatted(i)), b("v%05d".formatted(i))) != LeafPage.PutResult.SPLIT_NEEDED) {
            expected.put("k%05d".formatted(i), "v%05d".formatted(i));
            i++;
        }
        for (int j = 0; j < i; j += 2) {
            assertTrue(LeafPage.remove(p, b("k%05d".formatted(j))));
            expected.remove("k%05d".formatted(j));
        }
        assertTrue(Pages.deadBytes(p) > 0);

        int added = 0;
        while (LeafPage.put(p, b("n%05d".formatted(added)), b("w%05d".formatted(added))) != LeafPage.PutResult.SPLIT_NEEDED) {
            expected.put("n%05d".formatted(added), "w%05d".formatted(added));
            added++;
        }
        assertTrue(added > 0, "compaction should have freed room for more entries");
        assertEquals(expected, entriesOf(p));
    }

    @Test
    void splitWithEntryBalancesAndEmbedsThePendingInsert() {
        byte[] left = page();
        LeafPage.formatEmpty(left, 1, 77L, null);
        int i = 0;
        while (LeafPage.put(left, b("k%05d".formatted(i)), b("v%05d".formatted(i))) != LeafPage.PutResult.SPLIT_NEEDED) {
            i++;
        }
        int fullCount = i;
        byte[] oldFence = LeafPage.fenceBytes(left);

        byte[] right = page();
        byte[] pendingKey = b("k%05d".formatted(fullCount));
        byte[] promoted = LeafPage.splitWithEntry(left, right, 99L, oldFence, pendingKey, b("v-pending"));

        TreeMap<String, String> all = entriesOf(left);
        all.putAll(entriesOf(right));
        assertEquals(fullCount + 1, all.size());
        for (int j = 0; j < fullCount; j++) {
            assertEquals("v%05d".formatted(j), all.get("k%05d".formatted(j)));
        }
        assertEquals("v-pending", all.get(new String(pendingKey, StandardCharsets.UTF_8)));

        String leftBiggest = all.keySet().stream().filter(k -> entriesOf(left).containsKey(k))
                .max(String::compareTo).orElseThrow();
        assertEquals(leftBiggest, new String(promoted, StandardCharsets.UTF_8));
        assertArrayEquals(promoted, LeafPage.fenceBytes(left));
        assertNull(LeafPage.fenceBytes(right));
        assertEquals(99L, Pages.link(left));
        assertEquals(77L, Pages.link(right));
        assertEquals(1, Pages.height(left));
        assertEquals(1, Pages.height(right));

        assertTrue(Math.abs(Pages.count(left) - Pages.count(right)) <= 1);
        assertTrue(LeafPage.shouldGoRight(left, b("z")));
        assertFalse(LeafPage.shouldGoRight(left, promoted));
    }

    @Test
    void splitWithEntryEmbedsAGrowReplaceInTheHalves() {
        byte[] left = page();
        LeafPage.formatEmpty(left, 1, Pages.NO_LINK, null);
        int i = 0;
        while (LeafPage.put(left, b("k%05d".formatted(i)), b("v")) != LeafPage.PutResult.SPLIT_NEEDED) {
            i++;
        }
        byte[] bigValue = new byte[200];
        assertEquals(LeafPage.PutResult.SPLIT_NEEDED, LeafPage.put(left, b("k00000"), bigValue));

        byte[] right = page();
        LeafPage.splitWithEntry(left, right, 5L, null, b("k00000"), bigValue);

        TreeMap<String, String> all = entriesOf(left);
        all.putAll(entriesOf(right));
        assertEquals(i, all.size());
        assertEquals(200, all.get("k00000").length());
    }

    @Test
    void fenceSemanticsMatchRoutingRules() {
        byte[] p = leafWith("a", "1", "b", "2");
        LeafPage.setFence(p, b("m"));
        Pages.setLink(p, 42L);

        assertFalse(LeafPage.shouldGoRight(p, b("a")));
        assertFalse(LeafPage.shouldGoRight(p, b("m")));
        assertTrue(LeafPage.shouldGoRight(p, b("z")));
        assertEquals(42L, LeafPage.nextNode(p, b("z")));
        assertEquals(Pages.NO_LINK, LeafPage.nextNode(p, b("a")));
        assertArrayEquals(b("m"), LeafPage.fenceBytes(p));

        LeafPage.setFence(p, null);
        assertFalse(LeafPage.shouldGoRight(p, b("zzz")));
        assertNull(LeafPage.fenceBytes(p));
    }

    @Test
    void tombstoneRoutesRightViaLink() {
        byte[] p = page();
        LeafPage.formatTombstone(p, 3, 55L);

        assertTrue(Pages.isMarked(p));
        assertTrue(Pages.isLeaf(p));
        assertEquals(0, Pages.count(p));
        assertEquals(55L, LeafPage.nextNode(p, b("anything")));
        assertTrue(LeafPage.shouldGoRight(p, b("anything")));
        assertNull(LeafPage.fenceBytes(p));
    }

    @Test
    void mergeAbsorbsRightPageEntriesFenceAndLink() {
        byte[] left = leafWith("a", "1", "b", "2");
        LeafPage.setFence(left, b("b"));
        Pages.setLink(left, 8L);

        byte[] right = leafWith("c", "3", "d", "4");
        LeafPage.setFence(right, b("z"));
        Pages.setLink(right, 9L);

        assertTrue(LeafPage.canMerge(left, right));
        LeafPage.mergeFrom(left, right);

        assertEquals(Map.of("a", "1", "b", "2", "c", "3", "d", "4"), entriesOf(left));
        assertArrayEquals(b("z"), LeafPage.fenceBytes(left));
        assertEquals(9L, Pages.link(left));
    }

    @Test
    void canMergeRejectsWhenCombinedTooBig() {
        byte[] left = page();
        LeafPage.formatEmpty(left, 1, Pages.NO_LINK, null);
        int i = 0;
        while (LeafPage.put(left, b("k%05d".formatted(i)), b("v%05d".formatted(i))) != LeafPage.PutResult.SPLIT_NEEDED) {
            i++;
        }
        byte[] right = leafWith("x", "1", "y", "2", "z", "3");
        assertFalse(LeafPage.canMerge(left, right));
    }

    @Test
    void fuzzRandomPutsAndRemovesMatchTreeMap() {
        Random rnd = new Random(12345);
        byte[] p = page();
        LeafPage.formatEmpty(p, 1, Pages.NO_LINK, null);
        TreeMap<Long, byte[]> expected = new TreeMap<>();

        for (int round = 0; round < 20_000; round++) {
            long k = rnd.nextInt(400);
            byte[] kb = new byte[2];
            kb[0] = (byte) (k >>> 8);
            kb[1] = (byte) k;
            if (rnd.nextBoolean()) {
                byte[] v = new byte[rnd.nextInt(24)];
                rnd.nextBytes(v);
                LeafPage.PutResult r = LeafPage.put(p, kb, v);
                if (r == LeafPage.PutResult.SPLIT_NEEDED) {
                    continue;
                }
                expected.put(k, v);
            } else {
                assertEquals(expected.remove(k) != null, LeafPage.remove(p, kb));
            }
        }

        assertEquals(expected.size(), Pages.count(p));
        long prev = -1;
        for (int i = 0; i < Pages.count(p); i++) {
            byte[] kb = LeafPage.keyBytesAt(p, i);
            long k = ((kb[0] & 0xFF) << 8) | (kb[1] & 0xFF);
            assertTrue(k > prev, "slots must be strictly ordered");
            prev = k;
            assertArrayEquals(expected.get(k), LeafPage.valueAt(p, i));
            assertArrayEquals(expected.get(k), LeafPage.getValue(p, kb));
        }
    }

    @Test
    void fuzzSplitsAndMergesRoundTrip() {
        Random rnd = new Random(999);
        for (int iter = 0; iter < 50; iter++) {
            byte[] left = page();
            LeafPage.formatEmpty(left, 1, Pages.NO_LINK, null);
            TreeMap<String, byte[]> expected = new TreeMap<>();
            while (true) {
                String k = "k%06d".formatted(rnd.nextInt(1_000_000));
                byte[] v = new byte[rnd.nextInt(40)];
                rnd.nextBytes(v);
                if (expected.containsKey(k)) {
                    continue;
                }
                if (LeafPage.put(left, b(k), v) == LeafPage.PutResult.SPLIT_NEEDED) {
                    byte[] right = page();
                    LeafPage.splitWithEntry(left, right, 1234L, null, b(k), v);
                    expected.put(k, v);
                    TreeMap<String, byte[]> all = entriesOfBytes(left);
                    all.putAll(entriesOfBytes(right));
                    assertEquals(expected.size(), all.size());
                    for (Map.Entry<String, byte[]> e : expected.entrySet()) {
                        assertArrayEquals(e.getValue(), all.get(e.getKey()));
                    }
                    if (Pages.count(left) > 0 && Pages.count(right) > 0) {
                        String leftMax = new String(LeafPage.keyBytesAt(left, Pages.count(left) - 1), StandardCharsets.UTF_8);
                        String rightMin = new String(LeafPage.keyBytesAt(right, 0), StandardCharsets.UTF_8);
                        assertTrue(leftMax.compareTo(rightMin) < 0);
                    }
                    break;
                }
                expected.put(k, v);
            }
        }
    }
}
