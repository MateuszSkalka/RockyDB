package org.rockydb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NodeTest {

    @Test
    void needsSplitReturnsFalseAtMaxNodeSize() {
        int given = Node.MAX_NODE_SIZE;

        boolean result = Node.needsSplit(given);

        assertFalse(result);
    }

    @Test
    void needsSplitReturnsFalseBelowMaxNodeSize() {
        int given = Node.MAX_NODE_SIZE - 1;

        boolean result = Node.needsSplit(given);

        assertFalse(result);
    }

    @Test
    void needsSplitReturnsTrueAboveMaxNodeSize() {
        int given = Node.MAX_NODE_SIZE + 1;

        boolean result = Node.needsSplit(given);

        assertTrue(result);
    }

    @Test
    void accessorsReturnConstructorValues() {
        Node given = new TestNode(42L, true, 7, -1L);

        assertEquals(42L, given.id());
        assertTrue(given.isLeaf());
        assertEquals(7, given.height());
        assertEquals(-1L, given.link());
    }

    private static final class TestNode extends Node {
        TestNode(long id, boolean isLeaf, int height, long link) {
            super(id, isLeaf, height, link);
        }

        @Override
        public long nextNode(Value key) {
            return -1;
        }

        @Override
        public boolean isRightLink(long nodeId) {
            return false;
        }

        @Override
        public boolean shouldGoRight(Value key) {
            return false;
        }

        @Override
        public Value biggestKey() {
            return null;
        }
    }
}
