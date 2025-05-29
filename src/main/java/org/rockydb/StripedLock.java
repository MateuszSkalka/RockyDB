package org.rockydb;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StripedLock {
    private final int STRIPS = 1024;
    private final ReadWriteLock[] locks = new ReadWriteLock[STRIPS];

    public StripedLock() {
        for (int i = 0; i < locks.length; i++) {
            this.locks[i] = new ReentrantReadWriteLock();
        }
    }

    public void runInWriteLock(long nodeId, Runnable runnable) {
        ReadWriteLock lock = locks[computeStripForNodeId(nodeId)];
        lock.writeLock().lock();
        try {
            runnable.run();
        } finally {
            lock.writeLock().unlock();
        }
    }


    public void runInReadLock(long nodeId, Runnable runnable) {
        ReadWriteLock lock = locks[computeStripForNodeId(nodeId)];
        lock.readLock().lock();
        try {
            runnable.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    private int computeStripForNodeId(long nodeId) {
        return Long.hashCode(nodeId) & (STRIPS - 1);
    }
}
