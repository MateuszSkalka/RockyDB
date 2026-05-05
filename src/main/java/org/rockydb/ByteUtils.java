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
}
