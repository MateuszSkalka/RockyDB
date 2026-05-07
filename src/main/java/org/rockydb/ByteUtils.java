package org.rockydb;

public class ByteUtils {

    public static boolean readIsLeafFlag(byte flags) {
        return (flags & 1) > 0;
    }

    public static byte createFlags(boolean isLeaf) {
        byte flags = 0;
        if (isLeaf) {
            flags |= 1;
        }
        return flags;
    }
}
