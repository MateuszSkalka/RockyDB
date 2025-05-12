package org.rockydb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class NodeLockSupport {
    private final Map<Long, PerNodeLatch> pageLocks = new ConcurrentHashMap<>();

    public void lockNode(long nodeId) {
        PerNodeLatch newLatch = new PerNodeLatch(Thread.currentThread().threadId());
        PerNodeLatch existing;
        while ((existing = pageLocks.putIfAbsent(nodeId, newLatch)) != null) {
            try {
                existing.latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void unlockNode(long nodeId) {
        PerNodeLatch perNodeLatch = pageLocks.remove(nodeId);
        if (perNodeLatch != null) {
            if (perNodeLatch.owner != Thread.currentThread().threadId()) {
                throw new RuntimeException("Wrong Thread unlocked node");
            }
            perNodeLatch.latch.countDown();
        }
    }

    public void unlockAllNodesForThread() {
        long currentThread  =  Thread.currentThread().threadId();
        List<Long> nodesToUnlock = pageLocks.entrySet()
            .stream()
            .filter(entry -> entry.getValue().owner == currentThread)
            .map(Map.Entry::getKey)
            .toList();
        nodesToUnlock.forEach(this::unlockNode);
    }

    private static class PerNodeLatch {
        private final CountDownLatch latch;
        private final long owner;

        public PerNodeLatch(long owner) {
            this.latch = new CountDownLatch(1);
            this.owner = owner;
        }
    }
}
