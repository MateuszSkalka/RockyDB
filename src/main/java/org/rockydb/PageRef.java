package org.rockydb;

/**
 * A pinned frame holding page {@link #id}, with its {@code ioLock} read lock held from
 * acquisition until {@link #close()} — {@link #bytes()} is a consistent, stable view of
 * the page for the holder's lifetime. Reads run directly on the frame's array: no copy,
 * no deserialization. Writers mutate a shadow copy, so a reader is never blocked by an
 * in-progress write latch — only by the brief publish copy. Close promptly
 * (try-with-resources): holding it stalls a writer's publish.
 */
public interface PageRef extends AutoCloseable {
    long id();

    byte[] bytes();

    @Override
    void close();
}
