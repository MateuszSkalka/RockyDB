package org.rockydb;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class BranchNodeTest {

    private static Value v(String s) {
        return new Value(s.getBytes());
    }

    private static Value bytesKey(int len, int lastByte) {
        byte[] b = new byte[len];
        b[len - 1] = (byte) lastByte;
        return new Value(b);
    }

    private static Supplier<Long> noAllocation() {
        return () -> {
            throw new AssertionError("node id must not be allocated when no split occurs");
        };
    }

    @Test
    void nextNodeReturnsChildForKeyWithinRange() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        assertEquals(10L, given.nextNode(v("0")));
        assertEquals(10L, given.nextNode(v("a")));
        assertEquals(20L, given.nextNode(v("b")));
        assertEquals(20L, given.nextNode(v("c")));
    }

    @Test
    void nextNodeReturnsLastChildWhenKeyExceedsAllAndNoLink() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        long result = given.nextNode(v("z"));

        assertEquals(20L, result);
    }

    @Test
    void nextNodeFollowsLinkWhenKeyExceedsAllAndLinkExists() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, 99L);

        long result = given.nextNode(v("z"));

        assertEquals(99L, result);
    }

    @Test
    void shouldGoRightReturnsTrueOnlyWhenKeyExceedsMaxAndLinkExists() {
        BranchNode withLink = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, 99L);
        BranchNode noLink = new BranchNode(2L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        assertTrue(withLink.shouldGoRight(v("z")));
        assertFalse(withLink.shouldGoRight(v("a")));
        assertFalse(noLink.shouldGoRight(v("z")));
    }

    @Test
    void biggestKeyReturnsLastSeparator() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        Value result = given.biggestKey();

        assertEquals(v("c"), result);
    }

    @Test
    void isRightLinkMatchesLinkPointer() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a")}, new long[]{10}, 99L);

        assertTrue(given.isRightLink(99L));
        assertFalse(given.isRightLink(100L));
    }

    @Test
    void copyWithPropagatesChildSplitByFence() {
        // child 99 (separator "m") splits: survivor 99 takes new fence "b", new child 15 takes old fence "m".
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("m"), v("z")}, new long[]{10, 99, 20}, -1L);

        Node.CreationResult result = given.copyWith(v("m"), v("b"), 15L, v("m"), noAllocation());

        assertNull(result.right());
        assertNull(result.promotedValue());
        BranchNode left = (BranchNode) result.left();
        assertArrayEquals(new Value[]{v("a"), v("b"), v("m"), v("z")}, left.getKeys());
        assertArrayEquals(new long[]{10, 99, 15, 20}, left.getPointers());
    }

    @Test
    void copyWithUpdatesLeftmostSlotByFence() {
        // child 10 (separator "a") splits: promoted "m", new child 15 with fence "a".
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("m"), v("z")}, new long[]{10, 99, 20}, -1L);

        Node.CreationResult result = given.copyWith(v("a"), v("m"), 15L, v("a"), noAllocation());

        assertNull(result.right());
        BranchNode left = (BranchNode) result.left();
        long[] ptrs = left.getPointers();
        assertEquals(4, ptrs.length);
        assertEquals(10L, ptrs[0]);
        assertEquals(15L, ptrs[1]);
        assertEquals(99L, ptrs[2]);
        assertEquals(20L, ptrs[3]);
    }

    @Test
    void copyWithSplitsWhenExceedingMaxNodeSize() {
        Value big1 = new Value(new byte[4000]);
        Value bigMid = bytesKey(4000, 1);
        Value big2 = bytesKey(4000, 2);
        BranchNode given = new BranchNode(1L, 2, new Value[]{big1, big2}, new long[]{10, 20}, -1L);

        Node.CreationResult result = given.copyWith(big1, bigMid, 30L, big2, () -> 777L);

        assertNotNull(result.promotedValue());
        assertNotNull(result.right());
        assertEquals(777L, result.right().id());
        assertEquals(1L, result.left().id());
        assertEquals(777L, result.left().link());
    }

    @Test
    void removeDownlinkRemovesChildAndLeftSeparator() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("c"), v("e")}, new long[]{10, 20, 30}, -1L);

        BranchNode result = given.removeDownlink(v("c"));

        assertArrayEquals(new long[]{10, 30}, result.getPointers());
        assertArrayEquals(new Value[]{v("c"), v("e")}, result.getKeys());
    }

    @Test
    void removeDownlinkRemovesLeftmostChildAndItsHighKey() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("c"), v("e")}, new long[]{10, 20, 30}, -1L);

        BranchNode result = given.removeDownlink(v("a"));

        assertArrayEquals(new long[]{20, 30}, result.getPointers());
        assertArrayEquals(new Value[]{v("c"), v("e")}, result.getKeys());
    }

    @Test
    void removeDownlinkReturnsNullWhenFenceAbsent() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        assertNull(given.removeDownlink(v("z")));
    }

    @Test
    void removeDownlinkReturnsNullWhenItWouldEmptyTheNode() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a")}, new long[]{10}, -1L);

        assertNull(given.removeDownlink(v("a")));
    }

    @Test
    void isUnderfilledReturnsTrueForSmallBranch() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        assertTrue(given.isUnderfilled());
    }

    @Test
    void canMergeWithReturnsTrueWhenCombinedFits() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a")}, new long[]{10}, 2L);
        BranchNode right = new BranchNode(2L, 2, new Value[]{v("m")}, new long[]{30}, -1L);

        assertTrue(given.canMergeWith(right));
    }

    @Test
    void mergeWithRightConcatenatesKeysAndPointersAndInheritsRightLink() {
        BranchNode given = new BranchNode(1L, 2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, 2L);
        BranchNode right = new BranchNode(2L, 2, new Value[]{v("e"), v("g")}, new long[]{30, 40}, 99L);

        BranchNode merged = (BranchNode) given.mergeWithRight(right);

        assertEquals(1L, merged.id());
        assertArrayEquals(new Value[]{v("a"), v("c"), v("e"), v("g")}, merged.getKeys());
        assertArrayEquals(new long[]{10, 20, 30, 40}, merged.getPointers());
        assertEquals(99L, merged.link());
    }

    @Test
    void tombstoneIsMarkedEmptyAndRedirectsToSurvivor() {
        BranchNode tombstone = BranchNode.tombstone(5L, 2, 7L);

        assertEquals(5L, tombstone.id());
        assertTrue(tombstone.marked());
        assertEquals(0, tombstone.getKeys().length);
        assertEquals(7L, tombstone.link());
        assertEquals(7L, tombstone.nextNode(v("anything")));
    }
}
