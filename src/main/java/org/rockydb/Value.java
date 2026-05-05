package org.rockydb;

import java.util.Arrays;
import java.util.Objects;

public record Value(byte[] bytes) implements Comparable<Value> {

    @Override
    public int compareTo(Value o) {
        if (o == null) return 1;
        return Arrays.compare(this.bytes, o.bytes);
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
