package org.rockydb;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class LeafNodeTest {

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
    void getValueForKeyReturnsValueWhenKeyPresent() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, -1L);

        Value result = given.getValueForKey(v("c"));

        assertEquals(v("3"), result);
    }

    @Test
    void getValueForKeyReturnsNullWhenKeyAbsent() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, -1L);

        Value result = given.getValueForKey(v("b"));

        assertNull(result);
    }

    @Test
    void nextNodeStaysWhenKeyWithinNode() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, -1L);

        long result = given.nextNode(v("a"));

        assertEquals(-1L, result);
    }

    @Test
    void nextNodeFollowsLinkWhenKeyExceedsMax() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, 99L);

        long result = given.nextNode(v("z"));

        assertEquals(99L, result);
    }

    @Test
    void shouldGoRightReturnsFalseForEmptyNode() {
        LeafNode given = new LeafNode(1, 1, new Value[]{}, new Value[]{}, -1L);

        boolean result = given.shouldGoRight(v("a"));

        assertFalse(result);
    }

    @Test
    void shouldGoRightReturnsFalseWhenNoLink() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("c")}, new Value[]{v("3")}, -1L);

        boolean result = given.shouldGoRight(v("z"));

        assertFalse(result);
    }

    @Test
    void shouldGoRightReturnsTrueWhenKeyExceedsMaxAndLinkExists() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("c")}, new Value[]{v("3")}, 99L);

        boolean result = given.shouldGoRight(v("z"));

        assertTrue(result);
    }

    @Test
    void biggestKeyReturnsLastKey() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, -1L);

        Value result = given.biggestKey();

        assertEquals(v("c"), result);
    }

    @Test
    void isRightLinkMatchesLinkPointer() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, 99L);

        assertTrue(given.isRightLink(99L));
        assertFalse(given.isRightLink(100L));
    }

    @Test
    void copyWithInsertsNewKeyWithoutSplitting() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);

        Node.CreationResult result = given.copyWith(v("c"), v("3"), noAllocation());

        assertNull(result.right());
        assertNull(result.promotedValue());
        LeafNode left = (LeafNode) result.left();
        assertArrayEquals(new Value[]{v("a"), v("c")}, left.getKeys());
        assertArrayEquals(new Value[]{v("1"), v("3")}, left.getValues());
    }

    @Test
    void copyWithUpsertsValueForExistingKey() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, -1L);

        Node.CreationResult result = given.copyWith(v("c"), v("999"), noAllocation());

        assertNull(result.right());
        LeafNode left = (LeafNode) result.left();
        assertArrayEquals(new Value[]{v("a"), v("c")}, left.getKeys());
        assertArrayEquals(new Value[]{v("1"), v("999")}, left.getValues());
    }

    @Test
    void copyWithSplitsWhenExceedingMaxNodeSize() {
        Value keyA = new Value(new byte[4000]);
        Value keyB = bytesKey(4000, 1);
        Value valA = new Value(new byte[4000]);
        Value valB = new Value(new byte[4000]);
        LeafNode given = new LeafNode(1, 1, new Value[]{keyA}, new Value[]{valA}, -1L);

        Node.CreationResult result = given.copyWith(keyB, valB, () -> 555L);

        assertEquals(keyA, result.promotedValue());
        assertNotNull(result.right());
        assertEquals(555L, result.right().id());

        LeafNode left = (LeafNode) result.left();
        assertArrayEquals(new Value[]{keyA}, left.getKeys());
        assertEquals(555L, left.link());

        LeafNode right = (LeafNode) result.right();
        assertArrayEquals(new Value[]{keyB}, right.getKeys());
        assertEquals(-1L, right.link());
    }
}
