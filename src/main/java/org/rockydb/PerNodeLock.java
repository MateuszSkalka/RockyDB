package org.rockydb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PerNodeLock {
    private final ConcurrentMap<Long, QueueAwareLock> locks;

    public PerNodeLock() {
        this.locks = new ConcurrentHashMap<>();
    }


    public void lockNode(long nodeId) {
        QueueAwareLock latch = locks.compute(nodeId, (k, v) -> {
            if (v == null) return new QueueAwareLock();
            v.waitingThreads++;
            return v;
        });
        latch.lock.lock();
    }

    public void unlockNode(long nodeId) {
        locks.compute(nodeId, (k, v) -> {
            if (v == null) return null;
            v.waitingThreads--;
            v.lock.unlock();
            return v.waitingThreads == 0 ? null : v;
        });
    }

    private static class QueueAwareLock {
        private int waitingThreads = 1;
        private final Lock lock = new ReentrantLock();
    }
}
