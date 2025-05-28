package org.rockydb;

public class ByteUtils {

    public static boolean readIsLeafFlag(byte flags) {
        return (flags & 1) > 0;
    }

    public static boolean readIsLeftmostNodeFlag(byte flags) {
        return (flags & 2) > 0;
    }

    public static byte createFlags(boolean isLeaf, boolean isLeftmostNode) {
        byte flags = 0;
        if (isLeaf) {
            flags |= 1;
        }
        if (isLeftmostNode) {
            flags |= 2;
        }
        return flags;
    }

    public static byte[] toByteArray(long val) {
        return new byte[]{
            (byte) val,
            (byte) (val >> 8),
            (byte) (val >> 16),
            (byte) (val >> 24),
            (byte) (val >> 32),
            (byte) (val >> 40),
            (byte) (val >> 48),
            (byte) (val >> 56)
        };
    }

    public static long toLong(byte[] val) {
        return ((long) val[7] << 56)
            | ((long) val[6] & 0xff) << 48
            | ((long) val[5] & 0xff) << 40
            | ((long) val[4] & 0xff) << 32
            | ((long) val[3] & 0xff) << 24
            | ((long) val[2] & 0xff) << 16
            | ((long) val[1] & 0xff) << 8
            | ((long) val[0] & 0xff);
    }
}
