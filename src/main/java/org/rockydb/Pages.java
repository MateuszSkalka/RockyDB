package org.rockydb;

import java.util.Arrays;

final class Pages {
    private Pages() {}

    static final int HEADER_SIZE = 21;
    static final int FLAGS = 0;
    static final int COUNT = 1;
    static final int HEIGHT = 3;
    static final int LINK = 5;
    static final int UPPER = 13;
    static final int DEAD_BYTES = 15;
    static final int FENCE_OFF = 17;
    static final int FENCE_LEN = 19;

    static final int SLOT_SIZE = 4;
    static final int FLAG_LEAF = 1;
    static final int FLAG_MARKED = 2;
    static final int FLAG_INF_LAST = 4;
    static final int FENCE_INFINITE = 0xFFFF;
    static final long NO_LINK = -1L;

    static final int MAX_BODY = Store.PAGE_SIZE - HEADER_SIZE;

    static int u16(byte[] p, int off) {
        return ((p[off] & 0xFF) << 8) | (p[off + 1] & 0xFF);
    }

    static void putU16(byte[] p, int off, int v) {
        p[off] = (byte) (v >>> 8);
        p[off + 1] = (byte) v;
    }

    static int i32(byte[] p, int off) {
        return ((p[off] & 0xFF) << 24) | ((p[off + 1] & 0xFF) << 16)
                | ((p[off + 2] & 0xFF) << 8) | (p[off + 3] & 0xFF);
    }

    static void putI32(byte[] p, int off, int v) {
        p[off] = (byte) (v >>> 24);
        p[off + 1] = (byte) (v >>> 16);
        p[off + 2] = (byte) (v >>> 8);
        p[off + 3] = (byte) v;
    }

    static long i64(byte[] p, int off) {
        return ((long) i32(p, off) << 32) | (i32(p, off + 4) & 0xFFFFFFFFL);
    }

    static void putI64(byte[] p, int off, long v) {
        putI32(p, off, (int) (v >>> 32));
        putI32(p, off + 4, (int) v);
    }

    static boolean isLeaf(byte[] p) {
        return (p[FLAGS] & FLAG_LEAF) != 0;
    }

    static boolean isMarked(byte[] p) {
        return (p[FLAGS] & FLAG_MARKED) != 0;
    }

    static int count(byte[] p) {
        return u16(p, COUNT);
    }

    static void setCount(byte[] p, int n) {
        putU16(p, COUNT, n);
    }

    static int height(byte[] p) {
        return u16(p, HEIGHT);
    }

    static long link(byte[] p) {
        return i64(p, LINK);
    }

    static void setLink(byte[] p, long link) {
        putI64(p, LINK, link);
    }

    static int lower(byte[] p) {
        return HEADER_SIZE + count(p) * SLOT_SIZE;
    }

    static int contiguousFree(byte[] p) {
        return u16(p, UPPER) - lower(p);
    }

    static int deadBytes(byte[] p) {
        return u16(p, DEAD_BYTES);
    }

    static int liveBytes(byte[] p) {
        return (Store.PAGE_SIZE - u16(p, UPPER)) - deadBytes(p) + count(p) * SLOT_SIZE;
    }

    static boolean needsMerge(byte[] p) {
        return liveBytes(p) < Store.MERGE_THRESHOLD * MAX_BODY;
    }

    static int slotPos(int idx) {
        return HEADER_SIZE + idx * SLOT_SIZE;
    }

    static int keyOff(byte[] p, int idx) {
        return u16(p, slotPos(idx));
    }

    static int keyLen(byte[] p, int idx) {
        return u16(p, slotPos(idx) + 2);
    }

    static void setSlot(byte[] p, int idx, int off, int klen) {
        putU16(p, slotPos(idx), off);
        putU16(p, slotPos(idx) + 2, klen);
    }

    static void insertSlot(byte[] p, int idx, int off, int klen) {
        int n = count(p);
        System.arraycopy(p, slotPos(idx), p, slotPos(idx + 1), (n - idx) * SLOT_SIZE);
        setSlot(p, idx, off, klen);
        setCount(p, n + 1);
    }

    static void removeSlot(byte[] p, int idx) {
        int n = count(p);
        System.arraycopy(p, slotPos(idx + 1), p, slotPos(idx), (n - idx - 1) * SLOT_SIZE);
        setCount(p, n - 1);
    }

    static int compareRaw(byte[] p, int off, int len, byte[] key) {
        if (key == null) {
            return -1;
        }
        return Arrays.compareUnsigned(p, off, off + len, key, 0, key.length);
    }

    static int compareKeyAt(byte[] p, int idx, byte[] key) {
        return compareRaw(p, keyOff(p, idx), keyLen(p, idx), key);
    }

    static int find(byte[] p, byte[] key) {
        int lo = 0;
        int hi = count(p) - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = compareKeyAt(p, mid, key);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -(lo + 1);
    }
}
