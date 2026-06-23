package org.rockydb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

final class Clock {
    private static final int PROBES_BEFORE_BACKOFF = 64;

    private static final int MAX_SWEEPS = Frame.MAX_USAGE;

    private static final long BACKOFF_NANOS = 1_000L;

    private final Frame[] frames;
    private final AtomicInteger nextVictim = new AtomicInteger();

    Clock(Frame[] frames) {
        this.frames = frames;
    }

    Frame findVictim() {
        final int n = frames.length;
        int stalled = 0;

        for (int sweep = 0; sweep < MAX_SWEEPS; sweep++) {
            for (int step = 0; step < n; step++) {
                Frame frame = frames[Math.floorMod(nextVictim.getAndIncrement(), n)];
                if (frame.tryClaim()) {
                    if (frame.hasUsage()) {
                        frame.decrementUsage();
                        frame.releaseClaim();
                        stalled = 0;
                        continue;
                    }
                    return frame;
                }
                if (++stalled >= PROBES_BEFORE_BACKOFF) {
                    stalled = 0;
                    LockSupport.parkNanos(BACKOFF_NANOS);
                }
            }
        }

        for (int step = 0; step < n * MAX_SWEEPS; step++) {
            Frame frame = frames[Math.floorMod(nextVictim.getAndIncrement(), n)];
            if (frame.tryClaim()) {
                return frame;
            }
            if (++stalled >= PROBES_BEFORE_BACKOFF) {
                stalled = 0;
                LockSupport.parkNanos(BACKOFF_NANOS);
            }
        }
        throw new BufferExhaustedException(
                "No evictable frame found after " + (2 * MAX_SWEEPS) + " sweeps of " + n
                        + " frames; pool saturated by in-flight pins");
    }
}
