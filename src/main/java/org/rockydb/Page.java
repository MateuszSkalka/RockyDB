package org.rockydb;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Page {
    public static final short PAGE_SIZE = 1024 * 4;
    private final long pageNumber;
    private final PageHeaders pageHeaders;
    private ByteBuffer dataBuffer;
    private KeyValueTuple cachedValues = null;


    public Page(ByteBuffer buffer, long pageNumber) throws IOException {
        this.pageNumber = pageNumber;
        this.pageHeaders = new PageHeaders(buffer);
        this.dataBuffer = buffer;
    }

    public Page(PageHeaders headers, long pageNumber) throws IOException {
        this.pageNumber = pageNumber;
        this.dataBuffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        this.pageHeaders = headers;
    }

    public KeyValueTuple readValues() {
        if (cachedValues != null) {
            return cachedValues;
        }
        Value[] keys = new Value[pageHeaders.getElemCount()];
        int pSize = pageHeaders.getNodeType() == Node.LEAF ? pageHeaders.getElemCount() : pageHeaders.getElemCount() + 1;
        long[] valuePointers = new long[pSize];

        for (int i = 0; i < pageHeaders.getElemCount(); i++) {
            int nextValueSize = dataBuffer.getInt();
            byte[] bytes = new byte[nextValueSize];
            dataBuffer.get(bytes);
            keys[i] = new Value(bytes);
        }

        for (int i = 0; i < valuePointers.length; i++) {
            valuePointers[i] = dataBuffer.getLong();
        }

        this.cachedValues = new KeyValueTuple(keys, valuePointers);
        return cachedValues;
    }

    public void writeValues(Value[] keys, long[] pointers) {
        dataBuffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        pageHeaders.setElemCount(keys.length);
        int pageSize = PageHeaders.TOTAL_BYTES;
        for (int i = 0; i < keys.length; i++) {
            pageSize += keys.length + 4;
        }
        for (int i = 0; i < pointers.length; i++) {
            pageSize += 8;
        }
        pageHeaders.setPageSize(pageSize);
        pageHeaders.write(dataBuffer);
        for (int i = 0; i < keys.length; i++) {
            dataBuffer.putInt(keys[i].getVal().length);
            dataBuffer.put(keys[i].getVal());
        }
        for (int i = 0; i < pointers.length; i++) {
            dataBuffer.putLong(pointers[i]);
        }
    }

    public PageHeaders getHeaders() {
        return pageHeaders;
    }

    public ByteBuffer getBuffer() {
        return dataBuffer;
    }

    public long getPageNumber() {
        return pageNumber;
    }
}
