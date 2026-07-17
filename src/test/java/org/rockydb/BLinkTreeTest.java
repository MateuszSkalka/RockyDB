package org.rockydb;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BLinkTreeTest {

    /** Stub {@code allocate()} on a mock Store to draw from a deterministic id sequence,
     *  mirroring the real allocator. */
    private static void stubAllocator(Store mock, Supplier<Long> ids) {
        when(mock.allocate()).thenAnswer(inv -> ids.get());
    }

    private static Value v(String s) {
        return new Value(s.getBytes());
    }

    private static Value bytesKey(int len, int lastByte) {
        byte[] b = new byte[len];
        b[len - 1] = (byte) lastByte;
        return new Value(b);
    }

    @Test
    void getTraversesFromRootToLeafAndReturnsValue() {
        Store store = mock(Store.class);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);
        BranchNode root = new BranchNode(2L, 2, new Value[]{v("a")}, new long[]{1}, -1L);
        when(store.rootId()).thenReturn(2L);
        when(store.readNode(2L)).thenReturn(root);
        when(store.readNode(1L)).thenReturn(leaf);
        BLinkTree given = new BLinkTree(store);

        Value result = given.get(v("a"));

        assertEquals(v("1"), result);
        verify(store).readNode(2L);
        verify(store).readNode(1L);
    }

    @Test
    void getReturnsNullWhenKeyIsAbsent() {
        Store store = mock(Store.class);
        LeafNode root = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(root);
        BLinkTree given = new BLinkTree(store);

        Value result = given.get(v("z"));

        assertNull(result);
    }

    @Test
    void addValueInsertsIntoLeafWithoutSplit() {
        Store store = mock(Store.class);
        WriteHandle handle = mock(WriteHandle.class);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(handle);
        when(handle.get()).thenReturn(leaf);
        stubAllocator(store, () -> 99L);
        BLinkTree given = new BLinkTree(store);

        given.addValue(v("c"), v("3"));

        ArgumentCaptor<Node> setNode = ArgumentCaptor.forClass(Node.class);
        verify(handle).set(setNode.capture());
        assertArrayEquals(new Value[]{v("a"), v("c")}, ((LeafNode) setNode.getValue()).getKeys());
        verify(handle).close();
        verify(store, never()).writeNode(any());
        verify(store, never()).updateRootId(anyLong());
    }

    @Test
    void addValueReleasesLatchWhenExceptionOccurs() {
        Store store = mock(Store.class);
        WriteHandle handle = mock(WriteHandle.class);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(handle);
        when(handle.get()).thenThrow(new RuntimeException("boom"));
        stubAllocator(store, () -> 99L);
        BLinkTree given = new BLinkTree(store);

        assertThrows(RuntimeException.class, () -> given.addValue(v("c"), v("3")));

        verify(handle).close();
    }

    @Test
    void addValueFollowsLeafRightLinkBeforeInserting() {
        Store store = mock(Store.class);
        WriteHandle first = mock(WriteHandle.class);
        WriteHandle second = mock(WriteHandle.class);
        LeafNode leaf1 = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, 2L);
        LeafNode leaf2 = new LeafNode(2, 1, new Value[]{v("m")}, new Value[]{v("13")}, -1L);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(leaf1);
        when(store.latchForWrite(1L)).thenReturn(first);
        when(store.latchForWrite(2L)).thenReturn(second);
        when(first.get()).thenReturn(leaf1);
        when(second.get()).thenReturn(leaf2);
        stubAllocator(store, () -> 99L);
        BLinkTree given = new BLinkTree(store);

        given.addValue(v("z"), v("26"));

        verify(first).close();
        verify(first, never()).set(any());
        verify(second).set(any());
        verify(second).close();
    }

    @Test
    void addValuePropagatesLeafSplitWithoutRootSplit() {
        Store store = mock(Store.class);
        WriteHandle leafHandle = mock(WriteHandle.class);
        WriteHandle parentHandle = mock(WriteHandle.class);
        Value bigKey = new Value(new byte[4000]);
        Value newKey = bytesKey(4000, 1);
        Value bigVal = new Value(new byte[4000]);
        Value newVal = new Value(new byte[4000]);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{bigKey}, new Value[]{bigVal}, -1L, Value.infinity(), false);
        BranchNode parent = new BranchNode(2L, 2, new Value[]{Value.infinity()}, new long[]{1}, -1L);
        AtomicLong ids = new AtomicLong(50);
        when(store.rootId()).thenReturn(2L);
        when(store.readNode(2L)).thenReturn(parent);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(leafHandle);
        when(store.latchForWrite(2L)).thenReturn(parentHandle);
        when(leafHandle.get()).thenReturn(leaf);
        when(parentHandle.get()).thenReturn(parent);
        when(store.writeNode(any(Node.class))).thenAnswer(i -> i.getArgument(0));
        stubAllocator(store, ids::getAndIncrement);
        BLinkTree given = new BLinkTree(store);

        given.addValue(newKey, newVal);

        verify(store).writeNode(any());
        verify(leafHandle).set(any());
        verify(parentHandle).set(any());
        verify(leafHandle).close();
        verify(parentHandle).close();
        verify(store, never()).updateRootId(anyLong());
    }

    @Test
    void addValueCreatesNewRootWhenLeafSplitIsAtRoot() {
        Store store = mock(Store.class);
        WriteHandle handle = mock(WriteHandle.class);
        Value bigKey = new Value(new byte[4000]);
        Value newKey = bytesKey(4000, 1);
        Value bigVal = new Value(new byte[4000]);
        Value newVal = new Value(new byte[4000]);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{bigKey}, new Value[]{bigVal}, -1L);
        AtomicLong ids = new AtomicLong(50);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(handle);
        when(handle.get()).thenReturn(leaf);
        when(store.writeNode(any(Node.class))).thenAnswer(i -> i.getArgument(0));
        stubAllocator(store, ids::getAndIncrement);
        BLinkTree given = new BLinkTree(store);

        given.addValue(newKey, newVal);

        ArgumentCaptor<Node> written = ArgumentCaptor.forClass(Node.class);
        verify(store, times(2)).writeNode(written.capture());
        assertTrue(written.getAllValues().get(0) instanceof LeafNode);
        Node newRoot = written.getAllValues().get(1);
        assertTrue(newRoot instanceof BranchNode);
        assertEquals(51L, newRoot.id());
        verify(store).updateRootId(51L);
        verify(handle).set(any());
        verify(handle).close();
    }

    @Test
    void deleteRemovesKeyWithoutMergingWhenLeafStaysFull() {
        Store store = mock(Store.class);
        WriteHandle handle = mock(WriteHandle.class);
        Value key1 = bytesKey(2000, 1);
        Value key2 = bytesKey(2000, 2);
        Value val1 = new Value(new byte[2000]);
        Value val2 = new Value(new byte[2000]);
        // two ~2000-byte entries: after removing one, body stays above the merge threshold
        LeafNode leaf = new LeafNode(1, 1, new Value[]{key1, key2}, new Value[]{val1, val2}, 3L);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(handle);
        when(handle.get()).thenReturn(leaf);
        BLinkTree given = new BLinkTree(store);

        given.delete(key2);

        verify(handle).set(any());
        verify(store, never()).latchForWrite(3L); // right sibling never latched: no merge attempted
        verify(handle).close();
    }

    @Test
    void deleteDoesNotMergeRightmostLeaf() {
        Store store = mock(Store.class);
        WriteHandle handle = mock(WriteHandle.class);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, -1L);
        when(store.rootId()).thenReturn(1L);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(handle);
        when(handle.get()).thenReturn(leaf);
        BLinkTree given = new BLinkTree(store);

        given.delete(v("a")); // leaf becomes empty (underfilled) but has no right sibling

        verify(handle).set(any());
        verify(store, times(1)).latchForWrite(anyLong()); // only the leaf itself; no sibling/parent latch
        verify(handle).close();
    }

    @Test
    void deleteMergesUnderfilledLeafIntoRightSibling() {
        Store store = mock(Store.class);
        WriteHandle leafHandle = mock(WriteHandle.class);
        WriteHandle rightHandle = mock(WriteHandle.class);
        WriteHandle parentHandle = mock(WriteHandle.class);
        // leaf 1 (high-key "m") -> right sibling leaf 3 (high-key "z"); parent branch 2 covers both
        // leaf 1 (fence "m") -> right sibling leaf 3 (rightmost, fence +inf); parent keys [m, +inf]
        LeafNode leaf = new LeafNode(1, 1, new Value[]{v("a")}, new Value[]{v("1")}, 3L, v("m"), false);
        LeafNode right = new LeafNode(3, 1, new Value[]{v("m"), v("z")}, new Value[]{v("13"), v("26")}, -1L, Value.infinity(), false);
        BranchNode root = new BranchNode(2L, 2, new Value[]{v("m"), Value.infinity()}, new long[]{1, 3}, -1L);
        when(store.rootId()).thenReturn(2L);
        when(store.readNode(2L)).thenReturn(root);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(leafHandle);
        when(store.latchForWrite(3L)).thenReturn(rightHandle);
        when(store.latchForWrite(2L)).thenReturn(parentHandle);
        when(leafHandle.get()).thenReturn(leaf);
        when(rightHandle.get()).thenReturn(right);
        when(parentHandle.get()).thenReturn(root);
        BLinkTree given = new BLinkTree(store);

        given.delete(v("a")); // empties leaf 1 -> underfilled -> absorbs leaf 3

        // leaf handle is set twice: empty leaf after removal, then merged leaf with right's entries
        ArgumentCaptor<Node> leafSet = ArgumentCaptor.forClass(Node.class);
        verify(leafHandle, times(2)).set(leafSet.capture());
        LeafNode merged = (LeafNode) leafSet.getAllValues().get(1);
        assertEquals(1L, merged.id());
        assertArrayEquals(new Value[]{v("m"), v("z")}, merged.getKeys());
        assertArrayEquals(new Value[]{v("13"), v("26")}, merged.getValues());
        assertEquals(-1L, merged.link()); // inherited right's link

        // absorbed sibling becomes a tombstone redirecting to the survivor
        ArgumentCaptor<Node> rightSet = ArgumentCaptor.forClass(Node.class);
        verify(rightHandle).set(rightSet.capture());
        LeafNode tombstone = (LeafNode) rightSet.getValue();
        assertEquals(3L, tombstone.id());
        assertTrue(tombstone.marked());
        assertEquals(1L, tombstone.link());

        // parent drops child 3 and the separator to its left ("m"); child 1 inherits high-key "z"
        ArgumentCaptor<Node> parentSet = ArgumentCaptor.forClass(Node.class);
        verify(parentHandle).set(parentSet.capture());
        BranchNode updatedRoot = (BranchNode) parentSet.getValue();
        assertArrayEquals(new long[]{1L}, updatedRoot.getPointers());
        assertArrayEquals(new Value[]{Value.infinity()}, updatedRoot.getKeys());

        verify(leafHandle).close();
        verify(rightHandle).close();
        verify(parentHandle).close();
    }

    @Test
    void deleteDoesNotMergeWhenRightSiblingWouldOverflow() {
        Store store = mock(Store.class);
        WriteHandle leafHandle = mock(WriteHandle.class);
        WriteHandle rightHandle = mock(WriteHandle.class);
        // leaf 1 holds a ~1000-byte entry plus "b"; after deleting "b" it is underfilled (body ~2016),
        // but right sibling 3 holds a ~4000-byte entry so combined (~10024) would overflow MAX_NODE_SIZE
        Value keepKey = new Value(bigBytes(1000, (byte) 1));
        Value keepVal = new Value(new byte[1000]);
        Value bigKey = new Value(bigBytes(4000, (byte) 0x7A));
        Value bigVal = new Value(new byte[4000]);
        LeafNode leaf = new LeafNode(1, 1, new Value[]{keepKey, v("b")}, new Value[]{keepVal, v("2")}, 3L);
        LeafNode right = new LeafNode(3, 1, new Value[]{bigKey}, new Value[]{bigVal}, -1L);
        BranchNode root = new BranchNode(2L, 2, new Value[]{v("b"), v("z")}, new long[]{1, 3}, -1L);
        when(store.rootId()).thenReturn(2L);
        when(store.readNode(2L)).thenReturn(root);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(leafHandle);
        when(store.latchForWrite(3L)).thenReturn(rightHandle);
        when(leafHandle.get()).thenReturn(leaf);
        when(rightHandle.get()).thenReturn(right);
        BLinkTree given = new BLinkTree(store);

        given.delete(v("b"));

        verify(leafHandle).set(any());           // removal still applied
        verify(rightHandle, never()).set(any());  // sibling not tombstoned
        verify(store, never()).latchForWrite(2L); // parent untouched
        verify(rightHandle).close();
    }

    private static byte[] bigBytes(int len, byte first) {
        byte[] b = new byte[len];
        b[0] = first;
        return b;
    }
}
