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
    void freshStoreHasNoRoot() throws Exception {
        try (DiscStore given = openStore()) {
            assertEquals(-1, given.rootId());
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
    void allocateRawProducesMonotonicallyIncreasingIds() throws Exception {
        try (DiscStore given = openStore()) {
            long first = given.allocateRaw();
            long second = given.allocateRaw();
            long third = given.allocateRaw();

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
        long allocated = first.allocateRaw();
        first.updateRootId(allocated);
        first.close();

        try (DiscStore reopened = new DiscStore(dbFile)) {
            assertEquals(allocated, reopened.rootId());
        }
    }

    @Test
    void freeLogBootstrapRoundTrip() throws Exception {
        DiscStore first = openStore();
        first.writeFreeLogBootstrap(FreeLog.pack(11L, 3), FreeLog.pack(22L, 7));
        first.close();

        try (DiscStore reopened = new DiscStore(dbFile)) {
            assertEquals(FreeLog.pack(11L, 3), reopened.freeLogHeadPacked());
            assertEquals(FreeLog.pack(22L, 7), reopened.freeLogTailPacked());
        }
    }

    @Test
    void epochRoundTrip() throws Exception {
        DiscStore first = openStore();
        assertEquals(0, first.loadEpoch());
        first.writeEpoch(123L);
        first.close();

        try (DiscStore reopened = new DiscStore(dbFile)) {
            assertEquals(123L, reopened.loadEpoch());
        }
    }
}
