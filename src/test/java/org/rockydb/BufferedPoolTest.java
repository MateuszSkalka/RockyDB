package org.rockydb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BufferedPoolTest {

    private File dbFile;
    private DiscStore disc;
    private BufferedPool pool;

    @BeforeEach
    void setUp() throws IOException {
        dbFile = File.createTempFile("rockydb-pool-", ".db");
        dbFile.deleteOnExit();
        disc = new DiscStore(dbFile);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (pool != null) {
            pool.close();
            pool = null;
        }
        if (disc != null) {
            disc.close();
            disc = null;
        }
        if (dbFile != null && dbFile.exists()) {
            dbFile.delete();
        }
    }

    private static void writeMarker(Frame frame, byte marker) {
        frame.ioLock.writeLock().lock();
        try {
            frame.bytes[0] = marker;
            frame.setDirty(true);
        } finally {
            frame.ioLock.writeLock().unlock();
        }
    }

    @Test
    void acquireLoadsPageFromDisc() {
        pool = new BufferedPool(disc, 16);
        ByteBuffer page = ByteBuffer.wrap(new byte[Store.PAGE_SIZE]);
        page.put(0, (byte) 42);
        disc.writeRawPage(5L, page);

        Frame frame = pool.acquire(5L);
        frame.ioLock.readLock().lock();
        try {
            assertEquals(5L, frame.pageId());
            assertEquals((byte) 42, frame.bytes[0]);
        } finally {
            frame.ioLock.readLock().unlock();
            frame.unpin();
        }
    }

    @Test
    void repeatedAcquiresGenerateBufferHits() {
        pool = new BufferedPool(disc, 16);

        pool.acquire(5L).unpin();
        pool.acquire(5L).unpin();

        assertTrue(pool.getBufferHits() >= 1);
        assertTrue(pool.getBufferMisses() >= 1);
        double ratio = pool.getHitRatio();
        assertTrue(ratio > 0.0 && ratio <= 1.0);
    }

    @Test
    void tracksUsedAndDirtyFrames() {
        pool = new BufferedPool(disc, 16);

        assertEquals(16, pool.getNumFrames());
        assertEquals(0, pool.getUsedFrames());

        Frame frame = pool.acquire(5L);
        assertTrue(pool.getUsedFrames() >= 1);
        assertEquals(0, pool.getDirtyFrames());

        writeMarker(frame, (byte) 1);
        frame.unpin();
        assertTrue(pool.getDirtyFrames() >= 1);
    }

    @Test
    void throwsBufferExhaustedWhenPoolSaturatedByPins() {
        pool = new BufferedPool(disc, 1);

        Frame held = pool.acquire(1L);

        assertThrows(BufferExhaustedException.class, () -> pool.acquire(2L));

        held.unpin();
        pool.acquire(2L).unpin(); // evictable again once unpinned
    }

    @Test
    void evictionWritesDirtyFrameToDisc() {
        pool = new BufferedPool(disc, 1);

        Frame frame = pool.acquire(1L);
        writeMarker(frame, (byte) 77);
        frame.unpin();

        pool.acquire(2L).unpin(); // forces eviction of page 1

        assertEquals(1, pool.getEvictions());
        assertEquals((byte) 77, disc.readRawPage(1L).get(0));
    }

    @Test
    void closeFlushesDirtyFramesToDisc() throws Exception {
        pool = new BufferedPool(disc, 16);

        Frame frame = pool.acquire(3L);
        writeMarker(frame, (byte) 99);
        frame.unpin();

        pool.close();
        pool = null;

        assertEquals((byte) 99, disc.readRawPage(3L).get(0));
    }
}
