package org.rockydb;

import java.util.Arrays;

public record Value(byte[] bytes) implements Comparable<Value> {

    @Override
    public int compareTo(Value o) {
        if (o == null) return 1;
        return Arrays.compare(this.bytes, o.bytes);
    }
}
