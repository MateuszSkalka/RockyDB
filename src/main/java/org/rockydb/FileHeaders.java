package org.rockydb;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class FileHeaders {
    public static final int TOTAL_BYTES = 4;
    private AtomicInteger pagesCount;


    public FileHeaders(ByteBuffer byteBuffer) {
        this.pagesCount = new AtomicInteger(byteBuffer.getInt());
    }

    public void write(ByteBuffer byteBuffer) {
        byteBuffer.putInt(pagesCount.get());
    }

    public FileHeaders() {
        this.pagesCount = new AtomicInteger(1);
    }

    public int getAndIncrementPagesCount() {
        return pagesCount.getAndIncrement();
    }
}
