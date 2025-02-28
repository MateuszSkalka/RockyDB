package org.rockydb;

import java.nio.ByteBuffer;

public class PageHeaders {
    public static final int TOTAL_BYTES = 9;
    private byte nodeType;
    private int elemCount;
    private int pageSize;


    public PageHeaders(ByteBuffer byteBuffer) {
        this.nodeType = byteBuffer.get();
        this.elemCount = byteBuffer.getInt();
        this.pageSize = byteBuffer.getInt();
    }

    public PageHeaders(byte nodeType, int elemCount, int pageSize) {
        this.nodeType = nodeType;
        this.elemCount = elemCount;
        this.pageSize = pageSize;
    }

    public void write(ByteBuffer byteBuffer) {
        byteBuffer.put(nodeType);
        byteBuffer.putInt(elemCount);
        byteBuffer.putInt(pageSize);

    }

    public byte getNodeType() {
        return nodeType;
    }

    public void setNodeType(byte nodeType) {
        this.nodeType = nodeType;
    }

    public int getElemCount() {
        return elemCount;
    }

    public void setElemCount(int elemCount) {
        this.elemCount = elemCount;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
