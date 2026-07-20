package org.rockydb;

import java.util.Arrays;

/**
 * Leaf page operations on the raw page byte array. No node objects are materialized:
 * binary search runs directly over the slot array, mutations happen in place under the
 * caller's latch ({@code ioLock} write lock + tree latch). All ops are static; the page
 * id is not stored in the page — it is carried by the {@code PageRef}/{@code PageWrite}.
 *
 * <p>Tuple layout: {@code key[keyLen] | valueLen i32 | value[valueLen]}.
 * The fence key is stored as a standalone tuple referenced from the header; it is never
 * aliased to a slot key.
 *
 * <p>{@link #put} preflights space before touching the page: {@code SPLIT_NEEDED} leaves
 * the page logically unmodified (a compaction may have run). The split itself is done by
 * {@link #splitWithEntry}, which embeds the pending entry in the byte-balanced halves —
 * the same semantics as the old copy-on-write "grow then split".
 */
final class LeafPage {
    private LeafPage() {}

    enum PutResult { INSERTED, REPLACED, SPLIT_NEEDED }

    // ---- formatting ----

    /** Formats an empty leaf into {@code p}; {@code fence == null} encodes +∞. */
    static void formatEmpty(byte[] p, int height, long link, byte[] fence) {
        p[Pages.FLAGS] = Pages.FLAG_LEAF;
        Pages.setCount(p, 0);
        Pages.putU16(p, Pages.HEIGHT, height);
        Pages.setLink(p, link);
        Pages.putU16(p, Pages.DEAD_BYTES, 0);
        if (fence == null) {
            Pages.putU16(p, Pages.UPPER, Store.PAGE_SIZE);
            Pages.putU16(p, Pages.FENCE_OFF, 0);
            Pages.putU16(p, Pages.FENCE_LEN, Pages.FENCE_INFINITE);
        } else {
            int upper = Store.PAGE_SIZE - fence.length;
            System.arraycopy(fence, 0, p, upper, fence.length);
            Pages.putU16(p, Pages.UPPER, upper);
            Pages.putU16(p, Pages.FENCE_OFF, upper);
            Pages.putU16(p, Pages.FENCE_LEN, fence.length);
        }
    }

    /** Marked tombstone routing to {@code survivorId} via link; fence +∞ (as before). */
    static void formatTombstone(byte[] p, int height, long survivorId) {
        p[Pages.FLAGS] = Pages.FLAG_LEAF | Pages.FLAG_MARKED;
        Pages.setCount(p, 0);
        Pages.putU16(p, Pages.HEIGHT, height);
        Pages.setLink(p, survivorId);
        Pages.putU16(p, Pages.UPPER, Store.PAGE_SIZE);
        Pages.putU16(p, Pages.DEAD_BYTES, 0);
        Pages.putU16(p, Pages.FENCE_OFF, 0);
        Pages.putU16(p, Pages.FENCE_LEN, Pages.FENCE_INFINITE);
    }

    // ---- reads ----

    static int find(byte[] p, byte[] key) {
        return Pages.find(p, key);
    }

    /** Materializes the value on a hit — the only allocation on the read path. */
    static Value getValue(byte[] p, byte[] key) {
        int idx = find(p, key);
        if (idx < 0) {
            return null;
        }
        return valueAt(p, idx);
    }

    static Value keyAt(byte[] p, int idx) {
        return new Value(keyBytesAt(p, idx));
    }

    static Value valueAt(byte[] p, int idx) {
        int off = Pages.keyOff(p, idx);
        int klen = Pages.keyLen(p, idx);
        int vlen = Pages.i32(p, off + klen);
        byte[] v = new byte[vlen];
        System.arraycopy(p, off + klen + Integer.BYTES, v, 0, vlen);
        return new Value(v);
    }

    static byte[] keyBytesAt(byte[] p, int idx) {
        int off = Pages.keyOff(p, idx);
        int klen = Pages.keyLen(p, idx);
        byte[] k = new byte[klen];
        System.arraycopy(p, off, k, 0, klen);
        return k;
    }

    /** Fence key bytes, or {@code null} for +∞. Small copy, used on write paths only. */
    static byte[] fenceBytes(byte[] p) {
        int fl = Pages.u16(p, Pages.FENCE_LEN);
        if (fl == Pages.FENCE_INFINITE) {
            return null;
        }
        byte[] f = new byte[fl];
        System.arraycopy(p, Pages.u16(p, Pages.FENCE_OFF), f, 0, fl);
        return f;
    }

    static boolean shouldGoRight(byte[] p, byte[] key) {
        if (Pages.isMarked(p)) {
            return true;
        }
        int fl = Pages.u16(p, Pages.FENCE_LEN);
        if (fl == Pages.FENCE_INFINITE || Pages.link(p) == Pages.NO_LINK) {
            return false;
        }
        return Pages.compareRaw(p, Pages.u16(p, Pages.FENCE_OFF), fl, key) < 0; // key > fence
    }

    /** Next page to visit for {@code key}: right link while marked or past the fence, else -1. */
    static long nextNode(byte[] p, byte[] key) {
        if (shouldGoRight(p, key)) {
            return Pages.link(p);
        }
        return Pages.NO_LINK;
    }

    static boolean isUnderfilled(byte[] p) {
        return Pages.needsMerge(p);
    }

    // ---- mutations ----

    /**
     * Inserts or replaces in place. {@code SPLIT_NEEDED} leaves the page logically
     * unmodified — the caller then splits via {@link #splitWithEntry}.
     */
    static PutResult put(byte[] p, byte[] key, byte[] value) {
        int idx = find(p, key);
        int tupleLen = key.length + Integer.BYTES + value.length;
        PutResult result = PutResult.INSERTED;
        if (idx >= 0) {
            result = PutResult.REPLACED;
            int oldTupleLen = tupleLenAt(p, idx);
            if (tupleLen <= oldTupleLen) {
                int off = Pages.keyOff(p, idx);
                Pages.putI32(p, off + key.length, value.length);
                System.arraycopy(value, 0, p, off + key.length + Integer.BYTES, value.length);
                if (oldTupleLen > tupleLen) {
                    Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + oldTupleLen - tupleLen);
                }
                return result;
            }
            // grow-replace: preflight (old tuple becomes reclaimable) before touching the page
            if (Pages.contiguousFree(p) + Pages.deadBytes(p) + oldTupleLen < Pages.SLOT_SIZE + tupleLen) {
                return PutResult.SPLIT_NEEDED;
            }
            Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + oldTupleLen);
            Pages.removeSlot(p, idx); // insertion point for the new tuple stays idx
        } else {
            idx = -(idx + 1);
        }
        if (!ensureSpace(p, Pages.SLOT_SIZE + tupleLen)) {
            return PutResult.SPLIT_NEEDED;
        }
        int upper = Pages.u16(p, Pages.UPPER) - tupleLen;
        System.arraycopy(key, 0, p, upper, key.length);
        Pages.putI32(p, upper + key.length, value.length);
        System.arraycopy(value, 0, p, upper + key.length + Integer.BYTES, value.length);
        Pages.putU16(p, Pages.UPPER, upper);
        Pages.insertSlot(p, idx, upper, key.length);
        return result;
    }

    static boolean remove(byte[] p, byte[] key) {
        int idx = find(p, key);
        if (idx < 0) {
            return false;
        }
        Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + tupleLenAt(p, idx));
        Pages.removeSlot(p, idx);
        return true;
    }

    /**
     * Splits {@code left}'s entries plus the pending {@code (key, value)} byte-balanced into
     * {@code left} (rewritten) and {@code right} (a blank page array). The pending entry
     * replaces an equal key or is inserted in sort order, so the halves always contain it —
     * one split, no retry loop, same semantics as the old copy-on-write split.
     *
     * @param oldFence left's fence before the split (captured by the caller via
     *                 {@link #fenceBytes}); becomes right's fence and the parent's
     *                 downlink key for right. {@code null} = +∞.
     * @return the promoted key (left's new biggest) as a byte copy for the parent insert
     */
    static byte[] splitWithEntry(byte[] left, byte[] right, long rightId,
                                 byte[] oldFence, byte[] key, byte[] value) {
        // gather live entries as copies (left is rewritten below), merged with the pending entry
        int n = Pages.count(left);
        byte[][] keys = new byte[n + 1][];
        byte[][] vals = new byte[n + 1][];
        int m = 0;
        boolean pending = true;
        for (int i = 0; i < n; i++) {
            byte[] k = keyBytesAt(left, i);
            if (pending) {
                int cmp = Arrays.compareUnsigned(k, key);
                if (cmp > 0) {
                    keys[m] = key;
                    vals[m++] = value;
                    pending = false;
                } else if (cmp == 0) {
                    keys[m] = key;
                    vals[m++] = value; // replace: pending entry takes the old slot
                    pending = false;
                    continue;
                }
            }
            keys[m] = k;
            vals[m++] = valueAt(left, i).bytes();
        }
        if (pending) {
            keys[m] = key;
            vals[m++] = value;
        }

        int total = 0;
        for (int i = 0; i < m; i++) {
            total += keys[i].length + Integer.BYTES + vals[i].length;
        }
        int mid = 0;
        int leftSize = keys[0].length + Integer.BYTES + vals[0].length;
        int rightSize = total - leftSize;
        while (mid < m - 1) {
            int next = keys[mid + 1].length + Integer.BYTES + vals[mid + 1].length;
            if (Math.abs(rightSize - leftSize) <= Math.abs((rightSize - next) - (leftSize + next))) {
                break;
            }
            mid++;
            leftSize += next;
            rightSize -= next;
        }

        int height = Pages.height(left);
        long oldLink = Pages.link(left);
        rewriteHalf(left, keys, vals, 0, mid + 1, height, rightId, keys[mid]);
        rewriteHalf(right, keys, vals, mid + 1, m, height, oldLink, oldFence);
        return keys[mid];
    }

    /** Formats {@code p} as a leaf holding {@code keys[from..to)} and fence {@code fence}. */
    private static void rewriteHalf(byte[] p, byte[][] keys, byte[][] vals, int from, int to,
                                    int height, long link, byte[] fence) {
        formatEmpty(p, height, link, fence);
        int upper = Pages.u16(p, Pages.UPPER);
        for (int i = from; i < to; i++) {
            int tupleLen = keys[i].length + Integer.BYTES + vals[i].length;
            upper -= tupleLen;
            System.arraycopy(keys[i], 0, p, upper, keys[i].length);
            Pages.putI32(p, upper + keys[i].length, vals[i].length);
            System.arraycopy(vals[i], 0, p, upper + keys[i].length + Integer.BYTES, vals[i].length);
            Pages.setSlot(p, i - from, upper, keys[i].length);
        }
        Pages.putU16(p, Pages.UPPER, upper);
        Pages.setCount(p, to - from);
    }

    /**
     * Replaces the fence, marking the old fence bytes dead. Fences are only rewritten on
     * split/merge (both rebuild or compact the page first), so contiguous space always
     * suffices here; throws otherwise (a fence duplicates key bytes — same practical
     * key-size limit as the old format).
     */
    static void setFence(byte[] p, byte[] fence) {
        int old = Pages.u16(p, Pages.FENCE_LEN);
        if (old != Pages.FENCE_INFINITE) {
            Pages.putU16(p, Pages.DEAD_BYTES, Pages.deadBytes(p) + old);
        }
        if (fence == null) {
            Pages.putU16(p, Pages.FENCE_OFF, 0);
            Pages.putU16(p, Pages.FENCE_LEN, Pages.FENCE_INFINITE);
            return;
        }
        if (!ensureSpace(p, fence.length)) {
            throw new IllegalStateException("fence key does not fit page: " + fence.length + " bytes");
        }
        int upper = Pages.u16(p, Pages.UPPER) - fence.length;
        System.arraycopy(fence, 0, p, upper, fence.length);
        Pages.putU16(p, Pages.UPPER, upper);
        Pages.putU16(p, Pages.FENCE_OFF, upper);
        Pages.putU16(p, Pages.FENCE_LEN, fence.length);
    }

    /** Conservative combined-fit check (left's old fence is double-counted). */
    static boolean canMerge(byte[] left, byte[] right) {
        return Pages.liveBytes(left) + Pages.liveBytes(right) <= Pages.MAX_BODY;
    }

    /** Absorbs right's entries, fence and link into left. Caller checks {@link #canMerge}. */
    static void mergeFrom(byte[] left, byte[] right) {
        compact(left);
        compact(right);
        int ln = Pages.count(left);
        int rn = Pages.count(right);
        int rfl = Pages.u16(right, Pages.FENCE_LEN);
        int rFenceLen = rfl == Pages.FENCE_INFINITE ? 0 : rfl;
        // right's tuples sit below its fence: [upperR, PAGE_SIZE - rFenceLen)
        int srcOff = Pages.u16(right, Pages.UPPER);
        int bytes = Store.PAGE_SIZE - rFenceLen - srcOff;
        int dstOff = Pages.u16(left, Pages.UPPER) - bytes;
        System.arraycopy(right, srcOff, left, dstOff, bytes);
        int shift = dstOff - srcOff;
        for (int i = 0; i < rn; i++) {
            Pages.setSlot(left, ln + i, Pages.keyOff(right, i) + shift, Pages.keyLen(right, i));
        }
        Pages.setCount(left, ln + rn);
        Pages.putU16(left, Pages.UPPER, dstOff);
        Pages.setLink(left, Pages.link(right));
        setFence(left, fenceBytes(right));
    }

    // ---- internals ----

    static int tupleLenAt(byte[] p, int idx) {
        int off = Pages.keyOff(p, idx);
        int klen = Pages.keyLen(p, idx);
        return klen + Integer.BYTES + Pages.i32(p, off + klen);
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

    /**
     * Repacks tuples in slot order below the fence at the top of the page; deadBytes → 0.
     * Fence-at-top keeps a page's live region contiguous at the top, which is what makes
     * {@link #mergeFrom}'s bottom-copy and hole-free accounting work.
     */
    static void compact(byte[] p) {
        byte[] scratch = new byte[Store.PAGE_SIZE]; // rare path, like PG's PageRepairFragmentation
        int n = Pages.count(p);
        int fl = Pages.u16(p, Pages.FENCE_LEN);
        int upper = Store.PAGE_SIZE;
        int newFenceOff = 0;
        if (fl != Pages.FENCE_INFINITE) {
            upper -= fl;
            System.arraycopy(p, Pages.u16(p, Pages.FENCE_OFF), scratch, upper, fl);
            newFenceOff = upper;
        }
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
        Pages.putU16(scratch, Pages.FENCE_OFF, newFenceOff);
        Pages.putU16(scratch, Pages.FENCE_LEN, fl);
        System.arraycopy(scratch, 0, p, 0, Store.PAGE_SIZE);
    }
}
