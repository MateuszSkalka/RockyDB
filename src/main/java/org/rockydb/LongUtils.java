package org.rockydb;

public class LongUtils {

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
        if (val.length == 1) {
            System.out.println("here");
        }
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
