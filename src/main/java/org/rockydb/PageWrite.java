package org.rockydb;

/**
 * A pinned frame holding page {@link #id}, with the frame's tree latch held from
 * acquisition until {@link #close()} (other writers are excluded for the whole window).
 * The holder mutates a private <b>shadow copy</b> of the page — {@link #bytes()} — via
 * the static {@code LeafPage}/{@code BranchPage} ops; readers keep reading the last
 * published version of the page concurrently and never block for the latch window, only
 * for the brief array copy that publishes the shadow over the frame bytes.
 * <p>
 * Call {@link #markDirty()} after any real mutation: it marks the frame dirty (flushed
 * on eviction/close), bumps clock usage, and publishes the shadow immediately — a merge
 * tombstone on another page may already route readers here, so the mutated content must
 * be visible before {@link #close()}. Anything mutated after the last
 * {@code markDirty()} is published at {@link #close()}; latched-but-unmodified pages
 * are never published and stay clean.
 */
public interface PageWrite extends AutoCloseable {
    long id();

    byte[] bytes();

    /** Marks the frame dirty (flushed on eviction/close), bumps its clock usage, and
     *  publishes the current shadow copy so readers can see the mutation. */
    void markDirty();

    @Override
    void close();
}
