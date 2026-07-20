package org.rockydb;

import java.util.Arrays;
import java.util.Objects;

public record Value(byte[] bytes) implements Comparable<Value> {
    private static final Value INFINITY = new Value(null); // null bytes → immutable, shareable

    public static Value infinity() {
        return INFINITY;
    }

    public boolean isInfinite() {
        return bytes == null;
    }

    @Override
    public int compareTo(Value o) {
        if (o == null) return 1;
        boolean thisInf = this.bytes == null;
        boolean oInf = o.bytes == null;
        if (thisInf && oInf) return 0;
        if (thisInf) return 1;  // +inf > everything
        if (oInf) return -1;    // everything < +inf
        return Arrays.compareUnsigned(this.bytes, o.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Value value = (Value) o;
        return Objects.deepEquals(bytes, value.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
