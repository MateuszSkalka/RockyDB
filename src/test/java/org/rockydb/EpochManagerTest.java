package org.rockydb;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class EpochManagerTest {

    @Test
    void safeEpochEqualsCurrentWhenQuiescent() {
        EpochManager m = new EpochManager(10);
        assertEquals(10L, m.current());
        assertEquals(10L, m.safeEpoch());
    }

    @Test
    void pageFreedAtEpochFIsSafeOnlyAfterEpochAdvancesPastF() {
        // a page freed at F is reusable only when F < safeEpoch() — strict, since a live op pinned at F holds safeEpoch == F
        EpochManager m = new EpochManager(0);
        long ticket = m.enter();
        assertEquals(0L, m.safeEpoch());
        assertFalse(0L < m.safeEpoch());

        m.exit(ticket);
        m.advance();
        assertTrue(0L < m.safeEpoch(), "after advancing past the retire epoch, the page is safe");
    }

    @Test
    void exitReleasesPinAndAdvancesSafeEpoch() {
        EpochManager m = new EpochManager(5);
        long t = m.enter();
        assertEquals(5L, m.safeEpoch());
        m.exit(t);
        assertEquals(m.current(), m.safeEpoch());
    }

    @Test
    void multipleActiveOpsTakeMinimum() {
        EpochManager m = new EpochManager(0);
        long a = m.enter();
        m.advance();
        long b = m.enter();

        assertEquals(0L, m.safeEpoch());

        m.exit(a);
        assertEquals(1L, m.safeEpoch());

        m.exit(b);
        assertEquals(m.current(), m.safeEpoch());
    }

    @Test
    void exitIsIdempotentAgainstUnknownTicket() {
        EpochManager m = new EpochManager(0);
        assertDoesNotThrow(() -> m.exit(999L));
    }

    @Test
    void entryReachingZeroIsRemovedButEpochCanBeReentered() {
        EpochManager m = new EpochManager(3);
        long t = m.enter();
        m.exit(t);
        assertEquals(m.current(), m.safeEpoch());

        long t2 = m.enter();
        assertEquals(3L, m.safeEpoch());
        m.exit(t2);
        assertEquals(m.current(), m.safeEpoch());
    }

    @Test
    void racingEnterAndExitNeverLosesAnActiveOp() throws Exception {
        // regression: exit removing a zeroed entry must not hide a concurrent enter at the same epoch
        EpochManager m = new EpochManager(0);
        int threads = 8, iterations = 20_000;
        CyclicBarrier start = new CyclicBarrier(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iterations; i++) {
                            long ticket = m.enter();
                            assertTrue(m.safeEpoch() <= ticket,
                                    "safeEpoch must never advance past a live op");
                            m.exit(ticket);
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "threads did not finish");
        } finally {
            pool.shutdownNow();
        }
        if (failure.get() != null) {
            fail(failure.get());
        }
        assertEquals(m.current(), m.safeEpoch());
    }
}
