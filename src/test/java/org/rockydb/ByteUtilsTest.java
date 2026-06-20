package org.rockydb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ByteUtilsTest {

    @Test
    void createFlagsSetsLeafBitWhenLeaf() {
        boolean given = true;

        byte result = ByteUtils.createFlags(given);

        assertEquals(1, result);
    }

    @Test
    void createFlagsClearsAllBitsWhenBranch() {
        boolean given = false;

        byte result = ByteUtils.createFlags(given);

        assertEquals(0, result);
    }

    @Test
    void readIsLeafFlagReturnsTrueForLeafFlag() {
        byte given = ByteUtils.createFlags(true);

        boolean result = ByteUtils.readIsLeafFlag(given);

        assertTrue(result);
    }

    @Test
    void readIsLeafFlagReturnsFalseForBranchFlag() {
        byte given = ByteUtils.createFlags(false);

        boolean result = ByteUtils.readIsLeafFlag(given);

        assertFalse(result);
    }

    @Test
    void readIsLeafFlagOnlyInspectsLowestBit() {
        byte given = (byte) 0b11111110;

        boolean result = ByteUtils.readIsLeafFlag(given);

        assertFalse(result);
    }
}
