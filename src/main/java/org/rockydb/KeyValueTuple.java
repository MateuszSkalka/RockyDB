package org.rockydb;

public record KeyValueTuple(
    Value[] keys,
    long[] valuePointers
) {
}
