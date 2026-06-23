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
    void copyWithInsertsSeparatorAndPointerWithoutSplit() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        Node.CreationResult result = given.copyWith(v("b"), 15, v("c"), noAllocation());

        assertNull(result.right());
        assertNull(result.promotedValue());
        BranchNode left = (BranchNode) result.left();
        assertArrayEquals(new Value[]{v("a"), v("b"), v("c")}, left.getKeys());
        assertArrayEquals(new long[]{10, 20, 15}, left.getPointers());
    }

    @Test
    void copyWithReplacesPointerWhenKeyAlreadyPresent() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        Node.CreationResult result = given.copyWith(v("a"), 99, v("c"), noAllocation());

        assertNull(result.right());
        BranchNode left = (BranchNode) result.left();
        assertArrayEquals(new Value[]{v("a"), v("c")}, left.getKeys());
        assertArrayEquals(new long[]{99, 20}, left.getPointers());
    }

    @Test
    void copyWithUpdatesBiggestKeyWhenNewMaxIsLarger() {
        BranchNode given = new BranchNode(1L,2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, -1L);

        Node.CreationResult result = given.copyWith(v("b"), 15, v("z"), noAllocation());

        assertNull(result.right());
        BranchNode left = (BranchNode) result.left();
        assertArrayEquals(new Value[]{v("a"), v("b"), v("z")}, left.getKeys());
        assertArrayEquals(new long[]{10, 20, 15}, left.getPointers());
        assertEquals(v("z"), left.biggestKey());
    }

    @Test
    void copyWithSplitsWhenExceedingMaxNodeSize() {
        Value big1 = new Value(new byte[4000]);
        Value bigMid = bytesKey(4000, 1);
        Value big2 = bytesKey(4000, 2);
        BranchNode given = new BranchNode(1L, 2, new Value[]{big1, big2}, new long[]{10, 20}, -1L);

        Node.CreationResult result = given.copyWith(bigMid, 30, big2, () -> 777L);

        assertEquals(bigMid, result.promotedValue());
        assertNotNull(result.right());
        assertEquals(777L, result.right().id());

        BranchNode left = (BranchNode) result.left();
        assertArrayEquals(new Value[]{big1, bigMid}, left.getKeys());
        assertArrayEquals(new long[]{10, 20}, left.getPointers());
        assertEquals(777L, left.link());

        BranchNode right = (BranchNode) result.right();
        assertArrayEquals(new Value[]{big2}, right.getKeys());
        assertArrayEquals(new long[]{30}, right.getPointers());
        assertEquals(-1L, right.link());
    }
}
