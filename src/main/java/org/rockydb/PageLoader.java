package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class PageLoader {
    private final File dbFile;
    private final FileChannel fileChannel;
    private final FileHeaders fileHeaders;

    public PageLoader(File dbFile) throws IOException {
        this.dbFile = dbFile;
        boolean createDb = !dbFile.exists();
        RandomAccessFile raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        if (createDb) {
            FileHeaders fileHeaders = new FileHeaders();
            this.fileHeaders = fileHeaders;
            ByteBuffer buffer = ByteBuffer.wrap(new byte[FileHeaders.TOTAL_BYTES]);
            fileHeaders.write(buffer);
            buffer.rewind();
            fileChannel.write(buffer, 0);
            Page rootPage = createPage(Node.LEAF);
            rootPage.writeValues(new Value[]{}, new long[] {});
            savePage(rootPage);

        } else {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[Page.PAGE_SIZE]);
            fileChannel.read(buffer, 0);
            buffer.rewind();
            this.fileHeaders = new FileHeaders(buffer);
        }
    }


    public Page getPage(long pageNumber) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[Page.PAGE_SIZE]);
            fileChannel.read(buffer, pageNumber * Page.PAGE_SIZE);
            buffer.rewind();
            return new Page(buffer, pageNumber);
        } catch (IOException ex) {
            throw new RuntimeException();
        }
    }

    public Page createPage(byte type) {
        try {
            return new Page(new PageHeaders(type, 0, PageHeaders.TOTAL_BYTES), fileHeaders.getAndIncrementPagesCount());
        } catch (IOException ex) {
            throw new RuntimeException();
        }
    }

    public void savePage(Page page) {
        try {
            ByteBuffer byteBuffer = page.getBuffer();
            byteBuffer.rewind();
            fileChannel.write(byteBuffer, Page.PAGE_SIZE * page.getPageNumber());
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
