package org.rockydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class DiscStoreTest {

    private File dbFile;

    private DiscStore openStore() throws IOException {
        dbFile = File.createTempFile("rockydb-disc-", ".db");
        dbFile.deleteOnExit();
        return new DiscStore(dbFile);
    }

    @AfterEach
    void tearDown() {
        if (dbFile != null && dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    void freshStoreInitializesAnEmptyRootLeaf() throws Exception {
        try (DiscStore given = openStore()) {
            long rootId = given.rootId();

            assertTrue(rootId >= 1);
            Node root = PageCodec.deserialize(rootId, given.readRawPage(rootId));
            assertTrue(root.isLeaf());
            assertEquals(1, root.height());
        }
    }

    @Test
    void writeAndReadRawPageRoundTrip() throws Exception {
        try (DiscStore given = openStore()) {
            ByteBuffer payload = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
            payload.put((byte) 1);
            payload.putShort((short) 2);
            payload.putShort((short) 3);

            given.writeRawPage(5L, payload);

            ByteBuffer read = given.readRawPage(5L);
            assertEquals((byte) 1, read.get());
            assertEquals(2, read.getShort());
            assertEquals(3, read.getShort());
        }
    }

    @Test
    void nodeIdGeneratorIsMonotonicallyIncreasing() throws Exception {
        try (DiscStore given = openStore()) {
            long first = given.nodeIdGenerator().get();
            long second = given.nodeIdGenerator().get();
            long third = given.nodeIdGenerator().get();

            assertTrue(first >= 1);
            assertEquals(first + 1, second);
            assertEquals(second + 1, third);
        }
    }

    @Test
    void updateRootIdIsObservableInMemory() throws Exception {
        try (DiscStore given = openStore()) {
            given.updateRootId(99L);

            assertEquals(99L, given.rootId());
        }
    }

    @Test
    void reopeningPersistsRootId() throws Exception {
        DiscStore first = openStore();
        long allocated = first.nodeIdGenerator().get();
        first.updateRootId(allocated);
        first.close();

        try (DiscStore reopened = new DiscStore(dbFile)) {
            assertEquals(allocated, reopened.rootId());
        }
    }
}
