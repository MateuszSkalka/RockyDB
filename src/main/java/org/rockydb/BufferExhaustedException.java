package org.rockydb;


public class BufferExhaustedException extends RuntimeException {

    public BufferExhaustedException(String message) {
        super(message);
    }
}
