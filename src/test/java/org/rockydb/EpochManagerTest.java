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
        // Core safety rule: a page freed at F is reusable only when F < safeEpoch(). While an op
        // pinned at F is active, safeEpoch == F, so F < F is false -> not safe.
        EpochManager m = new EpochManager(0);
        long ticket = m.enter();        // op pins epoch 0
        assertEquals(0L, m.safeEpoch());
        assertFalse(0L < m.safeEpoch());

        m.exit(ticket);
        // Quiescent, but current is still 0 -> 0 < 0 still false. The epoch must advance so that a
        // FUTURE op sees a safeEpoch strictly greater than F.
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
        long a = m.enter();              // epoch 0
        m.advance();                     // current -> 1
        long b = m.enter();              // epoch 1

        assertEquals(0L, m.safeEpoch()); // straggler at 0 keeps safeEpoch low

        m.exit(a);
        assertEquals(1L, m.safeEpoch()); // now the newer op is the oldest

        m.exit(b);
        assertEquals(m.current(), m.safeEpoch());
    }

    @Test
    void exitIsIdempotentAgainstUnknownTicket() {
        EpochManager m = new EpochManager(0);
        assertDoesNotThrow(() -> m.exit(999L)); // never entered
    }

    @Test
    void entryReachingZeroIsRemovedButEpochCanBeReentered() {
        EpochManager m = new EpochManager(3);
        long t = m.enter();
        m.exit(t);
        assertEquals(m.current(), m.safeEpoch()); // entry is gone, not left at zero

        long t2 = m.enter(); // currentEpoch never advanced, so this re-enters the same epoch
        assertEquals(3L, m.safeEpoch()); // and it must be tracked again
        m.exit(t2);
        assertEquals(m.current(), m.safeEpoch());
    }

    @Test
    void racingEnterAndExitNeverLosesAnActiveOp() throws Exception {
        // Regression test for the exit-vs-enter race: exit decrements to 0 and removes the entry
        // while a concurrent enter at the same epoch has already grabbed the counter. The op must
        // stay visible, i.e. safeEpoch() may never pass a live op's ticket.
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
        assertEquals(m.current(), m.safeEpoch()); // all entries exited, nothing leaked
    }
}
