package org.rockydb;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class BLinkTreeTest {

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
        when(store.nodeIdGenerator()).thenReturn(() -> 99L);
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
        when(store.nodeIdGenerator()).thenReturn(() -> 99L);
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
        when(store.nodeIdGenerator()).thenReturn(() -> 99L);
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
        LeafNode leaf = new LeafNode(1, 1, new Value[]{bigKey}, new Value[]{bigVal}, -1L);
        BranchNode parent = new BranchNode(2L, 2, new Value[]{v("m")}, new long[]{1}, -1L);
        AtomicLong ids = new AtomicLong(50);
        when(store.rootId()).thenReturn(2L);
        when(store.readNode(2L)).thenReturn(parent);
        when(store.readNode(1L)).thenReturn(leaf);
        when(store.latchForWrite(1L)).thenReturn(leafHandle);
        when(store.latchForWrite(2L)).thenReturn(parentHandle);
        when(leafHandle.get()).thenReturn(leaf);
        when(parentHandle.get()).thenReturn(parent);
        when(store.writeNode(any(Node.class))).thenAnswer(i -> i.getArgument(0));
        when(store.nodeIdGenerator()).thenReturn(ids::getAndIncrement);
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
        when(store.nodeIdGenerator()).thenReturn(ids::getAndIncrement);
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
}
