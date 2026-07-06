package org.rockydb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public final class EpochManager {
    private static final long ADVANCE_EVERY = 1024;
    private final AtomicLong currentEpoch;
    private final ConcurrentMap<Long, AtomicLong> active = new ConcurrentHashMap<>();
    private final AtomicLong enterCountSinceAdvance = new AtomicLong();

    public EpochManager(long initialEpoch) {
        this.currentEpoch = new AtomicLong(initialEpoch);
    }


    public long enter() {
        long e = currentEpoch.get();
        active.computeIfAbsent(e, k -> new AtomicLong()).incrementAndGet();
        if (enterCountSinceAdvance.incrementAndGet() % ADVANCE_EVERY == 0) {
            currentEpoch.compareAndSet(e, e + 1);
        }
        return e;
    }

    public void exit(long ticket) {
        AtomicLong count = active.get(ticket);
        if (count == null) {
            return;
        }
        long after = count.decrementAndGet();
        if (after == 0) {
            active.remove(ticket, count);
        }
    }

    public long safeEpoch() {
        long min = Long.MAX_VALUE;
        for (long e : active.keySet()) {
            AtomicLong c = active.get(e);
            if (c != null && c.get() > 0 && e < min) {
                min = e;
            }
        }
        return min == Long.MAX_VALUE ? currentEpoch.get() : min;
    }

    public long current() {
        return currentEpoch.get();
    }

    public void advance() {
        currentEpoch.incrementAndGet();
    }
}
