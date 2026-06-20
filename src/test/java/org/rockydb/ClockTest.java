package org.rockydb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClockTest {

    @Test
    void findVictimReturnsFreeColdFrame() {
        Frame frame = new Frame(0);
        Clock given = new Clock(new Frame[]{frame});

        Frame result = given.findVictim();

        assertSame(frame, result);
        frame.releaseClaim();
    }

    @Test
    void findVictimGrantsSecondChancesUntilCold() {
        Frame frame = new Frame(0);
        frame.bumpUsage();
        frame.bumpUsage();
        frame.bumpUsage();
        Clock given = new Clock(new Frame[]{frame});

        Frame result = given.findVictim();

        assertSame(frame, result);
        assertFalse(frame.hasUsage());
        frame.releaseClaim();
    }

    @Test
    void findVictimSkipsPinnedFrameAndReturnsAnother() {
        Frame pinned = new Frame(0);
        pinned.pin();
        Frame cold = new Frame(1);
        Clock given = new Clock(new Frame[]{pinned, cold});

        Frame result = given.findVictim();

        assertSame(cold, result);
        result.releaseClaim();
        pinned.unpin();
    }

    @Test
    void findVictimThrowsWhenAllFramesPinned() {
        Frame frame = new Frame(0);
        frame.pin();
        Clock given = new Clock(new Frame[]{frame});

        assertThrows(BufferExhaustedException.class, given::findVictim);

        frame.unpin();
    }
}
