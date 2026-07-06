package org.rockydb;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

public final class PageAllocator {

    private final EpochManager epochs;
    private final FreeLog freeLog;
    private final LongSupplier allocateRaw;

    public PageAllocator(long persistedEpoch, FreeLog.FreeLogIO io, LongSupplier allocateRaw,
                         long headPacked, long tailPacked) {
        this.epochs = new EpochManager(persistedEpoch + 1);
        this.freeLog = new FreeLog(io, headPacked, tailPacked);
        this.allocateRaw = allocateRaw;
    }

    public long allocate() {
        return freeLog.acquireSafe(epochs.safeEpoch()).orElseGet(allocateRaw::getAsLong);
    }

    public void retire(long pageId) {
        freeLog.retire(pageId, epochs.current());
        epochs.advance();
    }

    public long enterOp() {
        return epochs.enter();
    }

    public void exitOp(long ticket) {
        epochs.exit(ticket);
    }

    long currentEpoch() {
        return epochs.current();
    }

    public Supplier<Long> idSupplier() {
        return this::allocate;
    }
}
