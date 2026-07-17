package org.rockydb;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class EpochManager {
    private static final long ADVANCE_EVERY = 1024;
    private final AtomicLong currentEpoch;
    private final ConcurrentSkipListMap<Long, Long> active = new ConcurrentSkipListMap<>();
    private final AtomicLong enterCountSinceAdvance = new AtomicLong();

    public EpochManager(long initialEpoch) {
        this.currentEpoch = new AtomicLong(initialEpoch);
    }


    public long enter() {
        long e = currentEpoch.get();
        active.merge(e, 1L, Long::sum);
        if (enterCountSinceAdvance.incrementAndGet() % ADVANCE_EVERY == 0) {
            currentEpoch.compareAndSet(e, e + 1);
        }
        return e;
    }

    public void exit(long ticket) {
        active.computeIfPresent(ticket, (k, v) -> v == 1 ? null : v - 1);
    }

    public long safeEpoch() {
        Map.Entry<Long, Long> oldest = active.firstEntry();
        return oldest == null ? currentEpoch.get() : oldest.getKey();
    }

    public long current() {
        return currentEpoch.get();
    }

    public void advance() {
        currentEpoch.incrementAndGet();
    }
}
