package org.rockydb;

import java.util.Arrays;

final class BranchPage {
    private BranchPage() {}

    static void formatEmpty(byte[] p, int height, long link) {
        p[Pages.FLAGS] = 0;
        Pages.setCount(p, 0);
        Pages.putU16(p, Pages.HEIGHT, height);
        Pages.setLink(p, link);
        Pages.putU16(p, Pages.UPPER, Store.PAGE_SIZE);
        Pages.putU16(p, Pages.DEAD_BYTES, 0);
        Pages.putU16(p, Pages.FENCE_OFF, 0);
        Pages.putU16(p, Pages.FENCE_LEN, 0);
    }

    static void formatTombstone(byte[] p, int height, long survivorId) {
        p[Pages.FLAGS] = Pages.FLAG_MARKED;
        Pages.setCount(p, 0);
        Pages.putU16(p, Pages.HEIGHT, height);
        Pages.setLink(p, survivorId);
        Pages.putU16(p, Pages.UPPER, Store.PAGE_SIZE);
        Pages.putU16(p, Pages.DEAD_BYTES, 0);
        Pages.putU16(p, Pages.FENCE_OFF, 0);
        Pages.putU16(p, Pages.FENCE_LEN, 0);
    }

    static int find(byte[] p, byte[] key) {
        int lo = 0;
        int hi = Pages.count(p) - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = compareKey(p, mid, key);
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

    private static int compareKey(byte[] p, int idx, byte[] key) {
        if (idx == Pages.count(p) - 1 && (p[Pages.FLAGS] & Pages.FLAG_INF_LAST) != 0) {
            return key == null ? 0 : 1;
        }
        return Pages.compareKeyAt(p, idx, key);
    }

    static long childAt(byte[] p, int idx) {
        return Pages.i64(p, Pages.keyOff(p, idx) + Pages.keyLen(p, idx));
    }

    static byte[] keyBytesAt(byte[] p, int idx) {
        int off = Pages.keyOff(p, idx);
        int klen = Pages.keyLen(p, idx);
        byte[] k = new byte[klen];
        System.arraycopy(p, off, k, 0, klen);
        return k;
    }

    static byte[] biggestKeyBytes(byte[] p) {
        if ((p[Pages.FLAGS] & Pages.FLAG_INF_LAST) != 0) {
            return null;
        }
        return keyBytesAt(p, Pages.count(p) - 1);
    }

    static boolean shouldGoRight(byte[] p, byte[] key) {
        if (Pages.isMarked(p)) {
            return true;
        }
        int n = Pages.count(p);
        return n > 0 && Pages.link(p) != Pages.NO_LINK && compareKey(p, n - 1, key) < 0;
    }

    static long nextNode(byte[] p, byte[] key) {
        if (Pages.isMarked(p)) {
            return Pages.link(p);
        }
        int idx = find(p, key);
        idx = idx < 0 ? -(idx + 1) : idx;
        int n = Pages.count(p);
        if (idx == n && Pages.link(p) != Pages.NO_LINK) {
            return Pages.link(p);
        } else if (idx == n) {
            return childAt(p, n - 1);
        } else {
            return childAt(p, idx);
        }
    }

    static boolean isUnderfilled(byte[] p) {
        return Pages.needsMerge(p);
    }

    static boolean insert(byte[] p, byte[] key, long childPtr) {
        if (key == null) {
            int need = Pages.SLOT_SIZE + Long.BYTES;
            if (!ensureSpace(p, need)) {
                return false;
            }
            int off = Pages.u16(p, Pages.UPPER) - Long.BYTES;
            Pages.putI64(p, off, childPtr);
            Pages.putU16(p, Pages.UPPER, off);
            Pages.insertSlot(p, Pages.count(p), off, 0);
            p[Pages.FLAGS] |= Pages.FLAG_INF_LAST;
            return true;
        }
        int idx = find(p, key);
        if (idx >= 0) {
            Pages.putI64(p, Pages.keyOff(p, idx) + Pages.keyLen(p, idx), childPtr);
            return true;
        }
        int ins = -(idx + 1);
        int need = Pages.SLOT_SIZE + key.length + Long.BYTES;
        if (!ensureSpace(p, need)) {
            return false;
        }
        int off = Pages.u16(p, Pages.UPPER) - (key.length + Long.BYTES);
        System.arraycopy(key, 0, p, off, key.length);
        Pages.putI64(p, off + key.length, childPtr);
        Pages.putU16(p, Pages.UPPER, off);
        Pages.insertSlot(p, ins, off, key.length);
        return true;
    }

    static boolean insertSplit(byte[] p, byte[] oldFence, byte[] promoted, long rightId) {
        int idx = find(p, oldFence);
        if (idx < 0) {
            throw new IllegalStateException("old fence not found in branch");
        }
        int oldLen = oldFence == null ? 0 : oldFence.length;
        int need = Pages.SLOT_SIZE + (promoted.length + Long.BYTES) + (oldLen + Long.BYTES);
        if (!ensureSpace(p, need)) {
            return false;
        }
        int oldOff = Pages.keyOff(p, idx);
        int oldKlen = Pages.keyLen(p, idx);
        long leftChild = Pages.i64(p, oldOff + oldKlen);
        Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + oldKlen + Long.BYTES);

        int off = Pages.u16(p, Pages.UPPER) - (promoted.length + Long.BYTES);
        System.arraycopy(promoted, 0, p, off, promoted.length);
        Pages.putI64(p, off + promoted.length, leftChild);
        Pages.putU16(p, Pages.UPPER, off);
        Pages.setSlot(p, idx, off, promoted.length);

        int off2 = Pages.u16(p, Pages.UPPER) - (oldLen + Long.BYTES);
        if (oldFence != null) {
            System.arraycopy(oldFence, 0, p, off2, oldFence.length);
        }
        Pages.putI64(p, off2 + oldLen, rightId);
        Pages.putU16(p, Pages.UPPER, off2);
        Pages.insertSlot(p, idx + 1, off2, oldLen);
        // oldFence == null: the new last slot carries ∞, so FLAG_INF_LAST just stays set
        return true;
    }

    static byte[] splitWithEntry(byte[] left, byte[] right, long rightId,
                                 byte[] oldFence, byte[] promoted, long newChildId) {
        int n = Pages.count(left);
        boolean infLast = (left[Pages.FLAGS] & Pages.FLAG_INF_LAST) != 0;
        byte[][] keys = new byte[n + 1][];
        long[] children = new long[n + 1];
        int m = 0;
        boolean replaced = false;
        for (int i = 0; i < n; i++) {
            byte[] k = keyBytesAt(left, i);
            long child = childAt(left, i);
            if (!replaced && (oldFence == null
                    ? infLast && i == n - 1
                    : Arrays.compareUnsigned(k, oldFence) == 0)) {
                keys[m] = promoted;
                children[m++] = child;
                keys[m] = oldFence == null ? new byte[0] : oldFence;
                children[m++] = newChildId;
                replaced = true;
                continue;
            }
            keys[m] = k;
            children[m++] = child;
        }
        if (!replaced) {
            throw new IllegalStateException("old fence not found in branch");
        }

        int total = 0;
        for (int i = 0; i < m; i++) {
            total += keys[i].length + Long.BYTES;
        }
        int mid = 0;
        int leftSize = keys[0].length + Long.BYTES;
        int rightSize = total - leftSize;
        while (mid < m - 1) {
            int next = keys[mid + 1].length + Long.BYTES;
            if (Math.abs(rightSize - leftSize) <= Math.abs((rightSize - next) - (leftSize + next))) {
                break;
            }
            mid++;
            leftSize += next;
            rightSize -= next;
        }

        int height = Pages.height(left);
        long oldLink = Pages.link(left);
        // ∞ is the gathered maximum, so it always lands in the right half
        rewriteHalf(left, keys, children, 0, mid + 1, height, rightId, false);
        rewriteHalf(right, keys, children, mid + 1, m, height, oldLink, infLast);
        return keys[mid];
    }

    private static void rewriteHalf(byte[] p, byte[][] keys, long[] children, int from, int to,
                                    int height, long link, boolean infLast) {
        formatEmpty(p, height, link);
        int upper = Pages.u16(p, Pages.UPPER);
        for (int i = from; i < to; i++) {
            int tupleLen = keys[i].length + Long.BYTES;
            upper -= tupleLen;
            System.arraycopy(keys[i], 0, p, upper, keys[i].length);
            Pages.putI64(p, upper + keys[i].length, children[i]);
            Pages.setSlot(p, i - from, upper, keys[i].length);
        }
        Pages.putU16(p, Pages.UPPER, upper);
        Pages.setCount(p, to - from);
        if (infLast) {
            p[Pages.FLAGS] |= Pages.FLAG_INF_LAST;
        }
    }

    static boolean removeDownlink(byte[] p, byte[] fence) {
        int idx = find(p, fence);
        if (idx < 0 || Pages.count(p) == 1) {
            return false;
        }
        if (idx > 0) {
            long leftPtr = childAt(p, idx - 1);
            Pages.putI64(p, Pages.keyOff(p, idx) + Pages.keyLen(p, idx), leftPtr);
            Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + tupleLenAt(p, idx - 1));
            Pages.removeSlot(p, idx - 1);
        } else {
            Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + tupleLenAt(p, 0));
            Pages.removeSlot(p, 0);
        }
        return true;
    }

    static boolean canMerge(byte[] left, byte[] right) {
        return Pages.liveBytes(left) + Pages.liveBytes(right) <= Pages.MAX_BODY;
    }

    static void mergeFrom(byte[] left, byte[] right) {
        compact(left);
        compact(right);
        int ln = Pages.count(left);
        int rn = Pages.count(right);
        int srcOff = Pages.u16(right, Pages.UPPER);
        int bytes = Store.PAGE_SIZE - srcOff;
        int dstOff = Pages.u16(left, Pages.UPPER) - bytes;
        System.arraycopy(right, srcOff, left, dstOff, bytes);
        int shift = dstOff - srcOff;
        for (int i = 0; i < rn; i++) {
            Pages.setSlot(left, ln + i, Pages.keyOff(right, i) + shift, Pages.keyLen(right, i));
        }
        Pages.setCount(left, ln + rn);
        Pages.putU16(left, Pages.UPPER, dstOff);
        Pages.setLink(left, Pages.link(right));
        if ((right[Pages.FLAGS] & Pages.FLAG_INF_LAST) != 0) {
            left[Pages.FLAGS] |= Pages.FLAG_INF_LAST;
        } else {
            left[Pages.FLAGS] &= ~Pages.FLAG_INF_LAST;
        }
    }

    static int tupleLenAt(byte[] p, int idx) {
        return Pages.keyLen(p, idx) + Long.BYTES;
    }

    private static boolean ensureSpace(byte[] p, int needed) {
        if (Pages.contiguousFree(p) >= needed) {
            return true;
        }
        if (Pages.contiguousFree(p) + Pages.deadBytes(p) < needed) {
            return false;
        }
        compact(p);
        return Pages.contiguousFree(p) >= needed;
    }

    static void compact(byte[] p) {
        byte[] scratch = new byte[Store.PAGE_SIZE];
        int n = Pages.count(p);
        int upper = Store.PAGE_SIZE;
        for (int i = 0; i < n; i++) {
            int tlen = tupleLenAt(p, i);
            upper -= tlen;
            System.arraycopy(p, Pages.keyOff(p, i), scratch, upper, tlen);
            Pages.setSlot(scratch, i, upper, Pages.keyLen(p, i));
        }
        scratch[Pages.FLAGS] = p[Pages.FLAGS];
        Pages.setCount(scratch, n);
        Pages.putU16(scratch, Pages.HEIGHT, Pages.height(p));
        Pages.putI64(scratch, Pages.LINK, Pages.link(p));
        Pages.putU16(scratch, Pages.UPPER, upper);
        Pages.putU16(scratch, Pages.DEAD_BYTES, 0);
        System.arraycopy(scratch, 0, p, 0, Store.PAGE_SIZE);
    }
}
