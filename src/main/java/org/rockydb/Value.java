package org.rockydb;

public class Value implements Comparable<Value> {
    private final byte[] val;

    public Value(byte[] val) {
        this.val = val;
    }

    public byte[] getVal() {
        return val;
    }

    @Override
    public int compareTo(Value o) {
        if (this.val.length > o.val.length) return 1;
        if (this.val.length < o.val.length) return -1;

        for (int i = 0; i < this.val.length; i++) {
            if (this.val[i] > o.val[i]) return 1;
            if (this.val[i] < o.val[i]) return -1;
        }
        return 0;
    }
}
