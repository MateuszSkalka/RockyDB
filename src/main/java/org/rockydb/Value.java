package org.rockydb;

public record Value(byte[] val) implements Comparable<Value> {

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
