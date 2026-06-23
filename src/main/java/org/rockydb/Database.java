package org.rockydb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Database implements Closeable {

    private final BufferedPool pool;
    private final BLinkTree catalog;
    private final ConcurrentMap<String, BLinkTree> tables = new ConcurrentHashMap<>();
    private final ReentrantLock catalogLock = new ReentrantLock();

    public Database(File dbFile, int numFrames) throws IOException {
        this.pool = new BufferedPool(dbFile, numFrames);
        this.catalog = new BLinkTree(pool); // catalog root = page 0 (StoreBackedRootRef)
    }

    public void createTable(String name) {
        Value nameKey = nameKey(name);
        catalogLock.lock();
        try {
            if (catalog.get(nameKey) != null) {
                throw new IllegalStateException("Table already exists: " + name);
            }
            long rootId = pool.nodeIdGenerator().get();
            pool.writeNode(new LeafNode(rootId, 1, new Value[]{}, new Value[]{}, -1L));
            catalog.addValue(nameKey, encodeRootId(rootId));
            tables.put(name, new BLinkTree(pool, new TableRootRef(nameKey, rootId)));
        } finally {
            catalogLock.unlock();
        }
    }

    public void dropTable(String name) {
        Value nameKey = nameKey(name);
        catalogLock.lock();
        try {
            if (catalog.get(nameKey) == null) {
                throw new IllegalStateException("No such table: " + name);
            }
            catalog.delete(nameKey);
            tables.remove(name);
            // NOTE: the dropped tree's pages are not reclaimed (no freelist; monotonic ids).
        } finally {
            catalogLock.unlock();
        }
    }

    public void insert(String table, Value key, Value value) {
        resolve(table).addValue(key, value);
    }

    public Value get(String table, Value key) {
        return resolve(table).get(key);
    }

    public void delete(String table, Value key) {
        resolve(table).delete(key);
    }

    public boolean tableExists(String name) {
        if (tables.containsKey(name)) {
            return true;
        }
        return catalog.get(nameKey(name)) != null;
    }

    long rootPageIdOf(String table) {
        Value v = catalog.get(nameKey(table));
        return v == null ? -1L : decodeRootId(v);
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
            Value nameKey = nameKey(table);
            Value rootVal = catalog.get(nameKey);
            if (rootVal == null) {
                throw new IllegalStateException("No such table: " + table);
            }
            BLinkTree tree = new BLinkTree(pool, new TableRootRef(nameKey, decodeRootId(rootVal)));
            tables.put(table, tree);
            return tree;
        } finally {
            catalogLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }

    private static Value nameKey(String name) {
        return new Value(name.getBytes(StandardCharsets.UTF_8));
    }

    private static Value encodeRootId(long id) {
        ByteBuffer b = ByteBuffer.allocate(Long.BYTES);
        b.putLong(id);
        return new Value(b.array());
    }

    private static long decodeRootId(Value v) {
        return ByteBuffer.wrap(v.bytes()).getLong();
    }

    private final class TableRootRef implements RootRef {
        private final Value nameKey;
        private final AtomicLong rootId;

        TableRootRef(Value nameKey, long initialRootId) {
            this.nameKey = nameKey;
            this.rootId = new AtomicLong(initialRootId);
        }

        @Override
        public long get() {
            return rootId.get();
        }

        @Override
        public void set(long id) {
            rootId.set(id);
            catalog.addValue(nameKey, encodeRootId(id));
        }
    }
}
