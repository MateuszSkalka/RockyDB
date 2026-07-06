package org.rockydb;

import org.junit.jupiter.api.Test;

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
}
