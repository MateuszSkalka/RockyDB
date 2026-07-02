package org.rockydb;

public class ByteUtils {

    private static final byte IS_LEAF = 1;
    private final static byte MARKED = 2;

    public static boolean readIsLeafFlag(byte flags) {
        return (flags & IS_LEAF) != 0;
    }

    public static boolean readMarkedFlag(byte flags) {
        return (flags & MARKED) != 0;
    }

    public static byte createFlags(boolean isLeaf) {
        return createFlags(isLeaf, false);
    }

    public static byte createFlags(boolean isLeaf, boolean marked) {
        byte flags = 0;
        if (isLeaf) {
            flags |= IS_LEAF;
        }
        if (marked) {
            flags |= MARKED;
        }
        return flags;
    }
}
