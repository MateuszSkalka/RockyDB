package org.rockydb;

/**
 * Thrown when the {@link BufferedPool}'s clock sweep cannot free a frame within a bounded number
 * of sweeps: every slot stays pinned or keeps getting its second-chance usage count bumped faster
 * than the sweep can cool it. In practice this means the pool is saturated by in-flight pins —
 * callers should size the pool larger or throttle concurrency rather than retry blindly.
 */
public class BufferExhaustedException extends RuntimeException {

    public BufferExhaustedException(String message) {
        super(message);
    }

    public BufferExhaustedException(String message, Throwable cause) {
        super(message, cause);
    }
}
