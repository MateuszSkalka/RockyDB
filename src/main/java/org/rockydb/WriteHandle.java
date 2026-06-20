package org.rockydb;

/**
 * A pinned, exclusively-latched tree node held for the duration of a write operation
 * (insert + split propagation). Returned by {@link Store#latchForWrite(long)}.
 * <p>
 * The backing frame is pinned (cannot be evicted) and its {@code treeLatch} is held, so the node's
 * identity and contents are stable for the handle's lifetime. The cache-miss disc read happens
 * <em>before</em> the latch is taken (inside {@code latchForWrite}), and {@link #set(Node)} writes
 * only to the in-memory frame (marking it dirty) — so no disc I/O occurs while the latch is held.
 * <p>
 * Callers MUST {@link #close()} the handle (in a {@code finally}) to release the latch and unpin the
 * frame. {@code get} re-parses a fresh, independent {@link Node} on each call (consistent with
 * {@link BufferedPool#readNode}), so the {@code copyWith} in-place mutation is safe.
 */
public interface WriteHandle extends AutoCloseable {

    /** Re-parse the node's current contents from the cached page (independent {@link Node}). */
    Node get();

    /** Serialize {@code node} into the cached page and mark it dirty. */
    void set(Node node);

    /** Release the tree latch and unpin the frame. Idempotent. */
    @Override
    void close();
}
