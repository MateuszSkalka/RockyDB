package org.rockydb;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class PageCodecTest {

    private static Value v(String s) {
        return new Value(s.getBytes());
    }

    private static Node roundTrip(long id, Node node) {
        ByteBuffer serialized = PageCodec.serialize(node);
        serialized.rewind();
        return PageCodec.deserialize(id, serialized);
    }

    @Test
    void serializeThenDeserializeLeafRoundTrips() {
        LeafNode given = new LeafNode(7, 1, new Value[]{v("a"), v("c")}, new Value[]{v("1"), v("3")}, 42L);

        Node result = roundTrip(7, given);

        assertTrue(result.isLeaf());
        assertEquals(7L, result.id());
        assertEquals(1, result.height());
        assertEquals(42L, result.link());
        LeafNode leaf = (LeafNode) result;
        assertArrayEquals(new Value[]{v("a"), v("c")}, leaf.getKeys());
        assertArrayEquals(new Value[]{v("1"), v("3")}, leaf.getValues());
    }

    @Test
    void serializeThenDeserializeBranchRoundTrips() {
        BranchNode given = new BranchNode(3L, 2, new Value[]{v("a"), v("c")}, new long[]{10, 20}, 42L);

        Node result = roundTrip(3, given);

        assertFalse(result.isLeaf());
        assertEquals(3L, result.id());
        assertEquals(2, result.height());
        assertEquals(42L, result.link());
        BranchNode branch = (BranchNode) result;
        assertArrayEquals(new Value[]{v("a"), v("c")}, branch.getKeys());
        assertArrayEquals(new long[]{10, 20}, branch.getPointers());
    }

    @Test
    void serializeFillsEntirePageSize() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);

        ByteBuffer result = PageCodec.serialize(given);

        assertEquals(Store.PAGE_SIZE, result.limit());
        assertEquals(Store.PAGE_SIZE, result.array().length);
    }

    @Test
    void deserializeProducesNodesThatDoNotShareMutableState() {
        LeafNode given = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);

        LeafNode first = (LeafNode) roundTrip(1, given);
        first.getValues()[0] = v("mutated");

        LeafNode second = (LeafNode) roundTrip(1, given);
        assertEquals(v("1"), second.getValues()[0]);
    }
}
