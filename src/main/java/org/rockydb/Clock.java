package org.rockydb;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Clock-sweep (second-chance) victim selection over a fixed {@link Frame} array.
 * <p>
 * The hand advances one frame per probe. A frame is a usable victim only when it is
 * <em>unpinned</em> (no in-flight users) and its usage count has been driven to zero by
 * successive sweeps. A pinned or recently-used frame is given a second chance: if it has
 * remaining usage it is decremented and the hand moves on; if it is pinned it is simply
 * skipped. This is the algorithm PostgreSQL uses for its buffer pool.
 */
final class Clock {
    /**
     * Number of consecutive probes that find no claimable frame before yielding briefly.
     * Bounds CPU use when the pool is temporarily fully pinned.
     */
    private static final int PROBES_BEFORE_BACKOFF = 64;

    /**
     * Number of full array sweeps spent in the second-chance phase (and again as the bound for
     * the fallback phase). Each sweep cools a frame by at most one usage unit, and usage is
     * capped at {@link Frame#MAX_USAGE}, so this many sweeps is enough to drive any quiescent
     * frame cold.
     */
    private static final int MAX_SWEEPS = Frame.MAX_USAGE;

    /** Nanoseconds to park when a sweep finds every frame pinned. Bounds CPU under saturation. */
    private static final long BACKOFF_NANOS = 1_000L;

    private final Frame[] frames;
    private final AtomicInteger nextVictim = new AtomicInteger();

    Clock(Frame[] frames) {
        this.frames = frames;
    }

    /**
     * Find and exclusively claim a victim frame.
     * <p>
     * Two phases:
     * <ol>
     *   <li><b>Second-chance sweeps.</b> Up to {@link #MAX_SWEEPS} full sweeps of the array,
     *       each frame visited once per sweep. An unpinned frame with residual usage burns one
     *       usage unit (a "second chance") and is released; an unpinned, cold frame is returned.
     *       The budget is counted in <em>sweeps</em>, not raw probes, so frames re-heated by
     *       concurrent readers are cooled deterministically rather than exhausting the budget.</li>
     *   <li><b>Fallback.</b> If every frame still carries residual usage after the sweeps
     *       (access is outrunning the sweep — the pool is hot, not saturated), claim any
     *       unpinned frame regardless of usage. Only a pool where every frame stays pinned
     *       through this phase is genuinely saturated, and only then does this throw.</li>
     * </ol>
     *
     * @return a frame claimed by the caller ({@link Frame#tryClaim()} succeeded, so
     *         {@code pinCount == 1}), ready to be repurposed. The caller owns the claim and
     *         must either repurpose+unpin or release it.
     * @throws BufferExhaustedException if every frame remains pinned through the second-chance
     *         sweeps and the fallback phase.
     */
    Frame findVictim() {
        final int n = frames.length;
        int stalled = 0;

        // Phase 1: bounded second-chance sweeps, counted in sweeps (not probes) so a hot pool
        // is cooled rather than spuriously failing.
        for (int sweep = 0; sweep < MAX_SWEEPS; sweep++) {
            for (int step = 0; step < n; step++) {
                Frame frame = frames[Math.floorMod(nextVictim.getAndIncrement(), n)];
                if (frame.tryClaim()) {
                    if (frame.hasUsage()) {
                        // Second chance: burn one usage unit and move the hand along.
                        frame.decrementUsage();
                        frame.releaseClaim();
                        stalled = 0;
                        continue;
                    }
                    return frame; // unpinned and cold — victim
                }
                // Pinned or transiently claimed by another evictor: try the next frame, with a
                // gentle backoff if the whole array is transiently busy.
                if (++stalled >= PROBES_BEFORE_BACKOFF) {
                    stalled = 0;
                    LockSupport.parkNanos(BACKOFF_NANOS);
                }
            }
        }

        // Phase 2: every surviving frame is hot, not free. Claim any unpinned frame, ignoring
        // residual usage — better to evict a hot frame than fail a valid operation. Only an
        // all-pinned pool (genuine saturation) can defeat this.
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
