package org.rockydb;


import org.rockydb.Node.CreationResult;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class BLinkTree {
    private final Store store;
    private final RootRef rootRef;

    public BLinkTree(Store store) {
        this(store, new StoreBackedRootRef(store));
    }

    public BLinkTree(Store store, RootRef rootRef) {
        this.store = store;
        this.rootRef = rootRef;
    }

    public Value get(Value key) {
        long rootId = rootRef.get();
        Node node = store.readNode(rootId);
        while (node.nextNode(key) != -1) {
            node = store.readNode(node.nextNode(key));
        }
        return ((LeafNode) node).getValueForKey(key);
    }

    public void addValue(Value key, Value value) {
        Deque<Long> ancestors = new ArrayDeque<>();
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
            if (!node.isRightLink(currentId)) {
                ancestors.push(node.id());
            }
            node = store.readNode(currentId);
        }

        WriteHandle handle = store.latchForWrite(currentId);
        try {
            LeafNode leaf = (LeafNode) handle.get();

            while (leaf.nextNode(key) != -1) {
                WriteHandle next = store.latchForWrite(leaf.nextNode(key));
                handle.close();
                handle = next;
                leaf = (LeafNode) handle.get();
            }

            boolean isRoot = rootRef.get() == leaf.id();
            CreationResult result = leaf.copyWith(key, value, store.nodeIdGenerator());

            while (result.promotedValue() != null) {
                Node rightChild = store.writeNode(result.right());
                handle.set(result.left());
                Node leftChild = result.left();

                if (isRoot) {
                    createNewRoot(leftChild, rightChild, result.promotedValue());
                    result = null;
                    break;
                }

                long parentId;
                if (!ancestors.isEmpty()) {
                    parentId = ancestors.pop();
                } else {
                    parentId = descendToLevel(result.promotedValue(), leftChild.height() + 1);
                }
                // Locate the survivor's parent by survivor id (not by a separator key): the survivor's
                // separator can be stale (a delete) or grown (a rightmost leaf that grew then split),
                // so a key-based lookup could hit the wrong slot. findParent follows tombstone
                // redirects and right-links and matches by id -- robust to both.
                WriteHandle parent = latchLiveNode(parentId, result.oldFence());
                try {
                    BranchNode branchParent = (BranchNode) parent.get();

                    isRoot = rootRef.get() == branchParent.id();
                    result = branchParent.copyWith(
                            result.oldFence(), result.promotedValue(),
                            rightChild.id(), rightChild.biggestKey(), store.nodeIdGenerator());
                } catch (RuntimeException e) {
                    parent.close();
                    throw e;
                }
                handle.close();
                handle = parent;
            }

            if (result != null) {
                handle.set(result.left());
            }
        } finally {
            handle.close();
        }
    }

    public void delete(Value key) {
        Deque<Long> ancestors = new ArrayDeque<>();
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (!node.isLeaf()) {
            currentId = node.nextNode(key);
            if (!node.isRightLink(currentId)) {
                ancestors.push(node.id());
            }
            node = store.readNode(currentId);
        }

        WriteHandle handle = store.latchForWrite(currentId);
        try {
            LeafNode leaf = (LeafNode) handle.get();
            while (leaf.nextNode(key) != -1) {
                WriteHandle next = store.latchForWrite(leaf.nextNode(key));
                handle.close();
                handle = next;
                leaf = (LeafNode) handle.get();
            }

            LeafNode updated = leaf.without(key);
            if (updated == null) {
                return; // key absent: nothing to remove or merge
            }
            handle.set(updated);

            // Cascade upward: at each level, if the node underflows, absorb its right sibling; that
            // removes a separator from the parent, which may underflow in turn (leaf and internal).
            Node current = updated;
            while (current.link() != -1 && current.isUnderfilled()) {
                Ascended ascended = tryAbsorbRight(handle, current, ancestors);
                if (ascended == null) {
                    break;
                }
                handle = ascended.handle;
                current = ascended.node;
            }
        } finally {
            handle.close();
        }
        collapseRootIfSingleChild();
    }

    /**
     * Absorb {@code current}'s right sibling into {@code current} (held via {@code handle}, underfilled).
     * The survivor is {@code current}; the sibling becomes a tombstone redirecting to it. Then drops
     * the sibling's separator from the parent and returns the updated parent (now latched) so the
     * cascade can ascend one level. Returns {@code null} if no merge happens; on {@code null} the
     * survivor latch stays held and is released by the caller.
     * <p>
     * Latch order: current -> right sibling -> parent (bottom-up, left-to-right), consistent with
     * {@link #addValue} and deadlock-free.
     */
    private Ascended tryAbsorbRight(WriteHandle handle, Node current, Deque<Long> ancestors) {
        long rightId = current.link();

        WriteHandle rightHandle = store.latchForWrite(rightId);
        Node right = rightHandle.get();
        if (right.marked() || !current.canMergeWith(right)) {
            rightHandle.close();
            return null; // already merged away, or combined would not fit
        }

        // Require the survivor and the sibling to share a parent and sit adjacent in it BEFORE moving
        // any data. Located by id (a separator value can be stale or grown); a cross-parent or
        // non-adjacent merge is skipped -- underflow is harmless.
        if (ancestors.isEmpty()) {
            rightHandle.close();
            return null;
        }
        Value rightFence = right.biggestKey();
        WriteHandle parentHandle = latchLiveNode(ancestors.peek(), rightFence);
        BranchNode parent = (BranchNode) parentHandle.get();
        Value[] parentKeys = parent.getKeys();
        int rightIdx = Arrays.binarySearch(parentKeys, rightFence);
        int curIdx = Arrays.binarySearch(parentKeys, current.biggestKey());
        if (rightIdx < 0 || curIdx < 0 || rightIdx != curIdx + 1) {
            parentHandle.close();
            rightHandle.close();
            return null; // cross-parent, non-adjacent, or absent: skip
        }

        long rightNodeId = right.id();
        int rightHeight = right.height();
        boolean rightIsLeaf = right.isLeaf();

        // half-merge: the held node absorbs its right sibling
        handle.set(current.mergeWithRight(right));
        // tombstone the absorbed sibling: its link now redirects to the survivor
        rightHandle.set(rightIsLeaf
                ? LeafNode.tombstone(rightNodeId, rightHeight, current.id())
                : BranchNode.tombstone(rightNodeId, rightHeight, current.id()));
        rightHandle.close();

        // complete-merge: drop the absorbed sibling's separator from the (shared) parent and ascend
        BranchNode updatedParent = parent.removeDownlink(rightFence);
        if (updatedParent == null) {
            parentHandle.close();
            return null; // removal would empty the parent: leave the tombstone to redirect
        }
        parentHandle.set(updatedParent);
        ancestors.pop();
        handle.close(); // release survivor latch; the parent latch takes over as we ascend
        return new Ascended(parentHandle, updatedParent);
    }

    /**
     * Latches the branch at {@code startId} and follows tombstone redirects and right-links until it
     * reaches the live branch covering {@code guideKey} (a fence value). With fence keys every
     * separator is exact (+∞ for the rightmost), so the lookup is unambiguous.
     */
    private WriteHandle latchLiveNode(Long startId, Value guideKey) {
        WriteHandle h = store.latchForWrite(startId);
        Node n = h.get();
        while (n.marked() || ((BranchNode) n).shouldGoRight(guideKey)) {
            WriteHandle next = store.latchForWrite(n.link());
            h.close();
            h = next;
            n = h.get();
        }
        return h;
    }

    private record Ascended(WriteHandle handle, Node node) {}

    /**
     * Height shrink: when the root is an internal node with a single child, promote the child to be
     * the new root. The old root is turned into a tombstone redirecting to the child (so any in-flight
     * descent/ascent that still holds the old root id follows the redirect), and {@code rootRef} is
     * repointed at the child. Repeats until the root is a leaf or has more than one child.
     * <p>
     * Concurrency: the root is latched and {@code rootRef} is re-verified under that latch, so this
     * serializes with {@link #createNewRoot} (which also runs under the root latch). Tombstoned roots
     * are never reclaimed, so a stale reader that reaches one is always redirected correctly.
     */
    private void collapseRootIfSingleChild() {
        while (true) {
            long rootId = rootRef.get();
            Node root = store.readNode(rootId);
            if (root.isLeaf() || root.marked()) {
                return; // leaf root, or already collapsed: nothing to do
            }
            BranchNode branch = (BranchNode) root;
            if (branch.getPointers().length != 1) {
                return; // multi-child root: keep
            }
            long childId = branch.getPointers()[0];

            WriteHandle h = store.latchForWrite(rootId);
            try {
                if (rootRef.get() != rootId) {
                    continue; // a concurrent createNewRoot replaced the root; re-evaluate
                }
                BranchNode rootAgain = (BranchNode) h.get();
                if (rootAgain.marked() || rootAgain.getPointers().length != 1) {
                    return; // concurrently collapsed, or no longer single-child
                }
                long child = rootAgain.getPointers()[0];
                // old root becomes a tombstone redirecting to its child; rootRef moves to the child
                h.set(BranchNode.tombstone(rootId, rootAgain.height(), child));
                rootRef.set(child);
            } finally {
                h.close();
            }
            // the child is now the root; loop in case it is also a single-child branch
        }
    }

    /**
     * Debug aid: returns null if the tree is structurally consistent, else a description of the first
     * problem found. Currently checks that every live leaf reachable via the leaf link chain is also
     * reachable via parent downlinks from the root (i.e. no orphaned live leaves).
     */
    String debugValidate() {
        java.util.Set<Long> viaParents = new java.util.HashSet<>();
        java.util.ArrayDeque<Long> stack = new java.util.ArrayDeque<>();
        stack.push(rootRef.get());
        while (!stack.isEmpty()) {
            long id = stack.pop();
            if (!viaParents.add(id)) {
                continue;
            }
            Node n = store.readNode(id);
            if (!n.isLeaf() && !n.marked()) {
                for (long p : ((BranchNode) n).getPointers()) {
                    stack.push(p);
                }
            }
        }
        // leftmost live leaf
        long cur = rootRef.get();
        while (true) {
            Node n = store.readNode(cur);
            if (n.isLeaf() || n.marked()) {
                break;
            }
            cur = ((BranchNode) n).getPointers()[0];
        }
        // walk live leaves via link chain (survivors' links skip tombstones)
        java.util.Set<Long> viaLinks = new java.util.HashSet<>();
        long leaf = cur;
        while (leaf != -1 && viaLinks.add(leaf)) {
            Node ln = store.readNode(leaf);
            leaf = ln.link();
        }
        java.util.Set<Long> orphaned = new java.util.HashSet<>(viaLinks);
        orphaned.removeAll(viaParents);
        if (!orphaned.isEmpty()) {
            return "ORPHANED live leaves (in link chain, not reachable from root): " + orphaned;
        }
        return null;
    }

    /**
     * Debug aid: returns the height of the current root (1 = single leaf). For testing height shrink.
     */
    int debugRootHeight() {
        return store.readNode(rootRef.get()).height();
    }

    /**
     * Debug aid: breadth-first dump of all nodes reachable from the root via child pointers and
     * right-links (latch-free reads). Not for production use.
     */
    String debugDump() {
        StringBuilder sb = new StringBuilder();
        java.util.Set<Long> visited = new java.util.HashSet<>();
        java.util.ArrayDeque<Long> queue = new java.util.ArrayDeque<>();
        queue.push(rootRef.get());
        while (!queue.isEmpty()) {
            long id = queue.pop();
            if (!visited.add(id)) {
                continue;
            }
            Node n = store.readNode(id);
            sb.append(id).append(": ").append(n.isLeaf() ? "L" : "B")
                    .append(n.marked() ? "*" : "").append(" h=").append(n.height())
                    .append(" link=").append(n.link());
            if (n.isLeaf()) {
                LeafNode l = (LeafNode) n;
                sb.append(" keys=").append(leafKeyIds(l));
            } else {
                BranchNode b = (BranchNode) n;
                sb.append(" keys=").append(branchKeyIds(b)).append(" ptrs=").append(java.util.Arrays.toString(b.getPointers()));
                for (long p : b.getPointers()) {
                    queue.push(p);
                }
            }
            sb.append('\n');
            if (n.link() != -1) {
                queue.push(n.link());
            }
        }
        return sb.toString();
    }

    private static String leafKeyIds(LeafNode l) {
        StringBuilder sb = new StringBuilder("[");
        Value[] keys = l.getKeys();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(",");
            byte[] b = keys[i].bytes();
            sb.append(b[b.length - 1] & 0xff);
        }
        return sb.append("]").toString();
    }

    private static String branchKeyIds(BranchNode b) {
        StringBuilder sb = new StringBuilder("[");
        Value[] keys = b.getKeys();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(",");
            byte[] by = keys[i].bytes();
            sb.append(by[by.length - 1] & 0xff);
        }
        return sb.append("]").toString();
    }

    private void createNewRoot(Node leftChild, Node rightChild, Value promotedValue) {
        Node newRoot = store.writeNode(new BranchNode(
                        store.nodeIdGenerator().get(),
                        leftChild.height() + 1,
                        new Value[]{promotedValue, rightChild.biggestKey()},
                        new long[]{leftChild.id(), rightChild.id()},
                        -1L
                )
        );
        rootRef.set(newRoot.id());
    }

    private long descendToLevel(Value key, int targetHeight) {
        long currentId = rootRef.get();
        Node node = store.readNode(currentId);
        while (node.height() > targetHeight) {
            currentId = node.nextNode(key);
            node = store.readNode(currentId);
        }
        return currentId;
    }

    private static final class StoreBackedRootRef implements RootRef {
        private final Store store;

        StoreBackedRootRef(Store store) {
            this.store = store;
        }

        @Override
        public long get() {
            return store.rootId();
        }

        @Override
        public void set(long rootId) {
            store.updateRootId(rootId);
        }
    }
}
