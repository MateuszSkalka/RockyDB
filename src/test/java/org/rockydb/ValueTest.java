package org.rockydb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ValueTest {

    @Test
    void compareToReturnsNegativeWhenBytesAreLexicographicallySmaller() {
        Value given = new Value(new byte[]{1});
        Value other = new Value(new byte[]{2});

        int result = given.compareTo(other);

        assertTrue(result < 0);
    }

    @Test
    void compareToReturnsPositiveWhenBytesAreLexicographicallyLarger() {
        Value given = new Value(new byte[]{9});
        Value other = new Value(new byte[]{2});

        int result = given.compareTo(other);

        assertTrue(result > 0);
    }

    @Test
    void compareToReturnsZeroWhenBytesAreEqual() {
        Value given = new Value(new byte[]{1, 2, 3});
        Value other = new Value(new byte[]{1, 2, 3});

        int result = given.compareTo(other);

        assertEquals(0, result);
    }

    @Test
    void compareToOrdersBytesAsUnsignedAcrossHighBitBoundary() {
        // Signed comparison would order 0x80 before 0x00 and corrupt key ordering.
        Value zero = new Value(new byte[]{0x00});
        Value highBit = new Value(new byte[]{(byte) 0x80});
        Value max = new Value(new byte[]{(byte) 0xFF});

        assertTrue(zero.compareTo(highBit) < 0, "0x00 must be < 0x80");
        assertTrue(highBit.compareTo(zero) > 0, "0x80 must be > 0x00");
        assertTrue(highBit.compareTo(max) < 0, "0x80 must be < 0xFF");
        assertTrue(max.compareTo(zero) > 0, "0xFF must be the largest byte");
    }

    @Test
    void compareToOrdersMultibyteKeysUnsigned() {
        Value small = new Value(new byte[]{0x00, 0x00});
        Value mid = new Value(new byte[]{0x00, (byte) 0x80});
        Value big = new Value(new byte[]{0x01, 0x00});

        assertTrue(small.compareTo(mid) < 0);
        assertTrue(mid.compareTo(big) < 0);
    }

    @Test
    void compareToTreatsNullAsSmaller() {
        Value given = new Value(new byte[]{1});

        int result = given.compareTo(null);

        assertEquals(1, result);
    }

    @Test
    void equalsReturnsTrueForEqualBytes() {
        Value given = new Value(new byte[]{1, 2});
        Value other = new Value(new byte[]{1, 2});

        boolean result = given.equals(other);

        assertTrue(result);
    }

    @Test
    void equalsReturnsFalseForDifferentBytes() {
        Value given = new Value(new byte[]{1, 2});
        Value other = new Value(new byte[]{1, 3});

        boolean result = given.equals(other);

        assertFalse(result);
    }

    @Test
    void equalsReturnsFalseForNull() {
        Value given = new Value(new byte[]{1});

        boolean result = given.equals(null);

        assertFalse(result);
    }

    @Test
    void equalsReturnsFalseForOtherType() {
        Value given = new Value(new byte[]{1});

        boolean result = given.equals("not a value");

        assertFalse(result);
    }

    @Test
    void hashCodeIsConsistentWithEquals() {
        Value given = new Value(new byte[]{5, 6, 7});
        Value equal = new Value(new byte[]{5, 6, 7});

        int firstHash = given.hashCode();
        int secondHash = equal.hashCode();

        assertEquals(firstHash, secondHash);
        assertEquals(given.hashCode(), given.hashCode());
    }
}
