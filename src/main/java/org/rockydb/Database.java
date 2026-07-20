package org.rockydb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Database implements Closeable {

    private final NodeStore store;
    private final BLinkTree catalog;
    private final ConcurrentMap<String, BLinkTree> tables = new ConcurrentHashMap<>();
    private final ReentrantLock catalogLock = new ReentrantLock();

    public Database(File dbFile, int numFrames) throws IOException {
        this.store = new NodeStore(dbFile, numFrames);
        this.catalog = new BLinkTree(store);
    }

    public void createTable(String name) {
        byte[] nameKey = nameKey(name);
        catalogLock.lock();
        try {
            if (catalog.get(nameKey) != null) {
                throw new IllegalStateException("Table already exists: " + name);
            }
            long rootId = store.allocate();
            try (PageWrite root = store.latchForWriteBlank(rootId)) {
                LeafPage.formatEmpty(root.bytes(), 1, Pages.NO_LINK, null);
                root.markDirty();
            }
            catalog.addValue(nameKey, encodeMeta(rootId, 0));
            tables.put(name, new BLinkTree(store, new TableRootRef(nameKey, rootId, 0)));
        } finally {
            catalogLock.unlock();
        }
    }

    public void dropTable(String name) {
        byte[] nameKey = nameKey(name);
        catalogLock.lock();
        try {
            byte[] rootVal = catalog.get(nameKey);
            if (rootVal == null) {
                throw new IllegalStateException("No such table: " + name);
            }
            long rootId = decodeRootId(rootVal);
            catalog.delete(nameKey);
            tables.remove(name);
            reclaimTreePages(rootId);
        } finally {
            catalogLock.unlock();
        }
    }

    private void reclaimTreePages(long rootId) {
        Set<Long> visited = new HashSet<>();
        ArrayDeque<Long> stack = new ArrayDeque<>();
        stack.push(rootId);
        long ticket = store.enterOp();
        try {
            while (!stack.isEmpty()) {
                long id = stack.pop();
                if (!visited.add(id)) {
                    continue;
                }
                try (PageRef ref = store.readPage(id)) {
                    byte[] p = ref.bytes();
                    if (!Pages.isLeaf(p) && !Pages.isMarked(p)) {
                        for (int i = 0; i < Pages.count(p); i++) {
                            stack.push(BranchPage.childAt(p, i));
                        }
                    }
                    if (Pages.link(p) != Pages.NO_LINK) {
                        stack.push(Pages.link(p));
                    }
                }
                store.retire(id);
            }
        } finally {
            store.exitOp(ticket);
        }
    }

    public void insert(String table, byte[] key, byte[] value) {
        resolve(table).addValue(key, value);
    }

    public byte[] get(String table, byte[] key) {
        return resolve(table).get(key);
    }

    public void delete(String table, byte[] key) {
        resolve(table).delete(key);
    }

    public long count(String table) {
        return resolve(table).count();
    }

    public boolean tableExists(String name) {
        if (tables.containsKey(name)) {
            return true;
        }
        return catalog.get(nameKey(name)) != null;
    }

    long rootPageIdOf(String table) {
        byte[] v = catalog.get(nameKey(table));
        return v == null ? -1L : decodeRootId(v);
    }

    long highWaterMark() {
        return store.highWaterMark();
    }

    private BLinkTree resolve(String table) {
        BLinkTree cached = tables.get(table);
        if (cached != null) {
            return cached;
        }
        catalogLock.lock();
        try {
            cached = tables.get(table);
            if (cached != null) {
                return cached;
            }
            byte[] nameKey = nameKey(table);
            byte[] rootVal = catalog.get(nameKey);
            if (rootVal == null) {
                throw new IllegalStateException("No such table: " + table);
            }
            BLinkTree tree = new BLinkTree(store, new TableRootRef(nameKey, decodeRootId(rootVal), decodeCount(rootVal)));
            tables.put(table, tree);
            return tree;
        } finally {
            catalogLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, BLinkTree> e : tables.entrySet()) {
            catalog.addValue(nameKey(e.getKey()), encodeMeta(rootPageIdOf(e.getKey()), e.getValue().count()));
        }
        store.close();
    }

    private static byte[] nameKey(String name) {
        return name.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] encodeMeta(long rootId, long count) {
        ByteBuffer b = ByteBuffer.allocate(2 * Long.BYTES);
        b.putLong(rootId);
        b.putLong(count);
        return b.array();
    }

    private static long decodeRootId(byte[] v) {
        return ByteBuffer.wrap(v).getLong();
    }

    private static long decodeCount(byte[] v) {
        return ByteBuffer.wrap(v).getLong(Long.BYTES);
    }

    private final class TableRootRef implements RootRef {
        private final byte[] nameKey;
        private final AtomicLong rootId;
        private final AtomicLong count;

        TableRootRef(byte[] nameKey, long initialRootId, long initialCount) {
            this.nameKey = nameKey;
            this.rootId = new AtomicLong(initialRootId);
            this.count = new AtomicLong(initialCount);
        }

        @Override
        public long get() {
            return rootId.get();
        }

        @Override
        public void set(long id) {
            rootId.set(id);
            catalog.addValue(nameKey, encodeMeta(id, count.get()));
        }

        @Override
        public long count() {
            return count.get();
        }

        @Override
        public void addToCount(long delta) {
            count.addAndGet(delta);
        }
    }
}
