package org.rockydb;

import java.util.Arrays;

public class Node {
    public static final byte UNUSED = 0;
    public static final byte BRANCH = 1;
    public static final byte LEAF = 2;

    private final PageLoader pageLoader;
    public Page page;
    public Value[] keys;
    public long[] pointers;
    public byte type;


    public Node(PageLoader pageLoader, long pageNumber) {
        this.pageLoader = pageLoader;
        this.page = pageLoader.getPage(pageNumber);
        this.type = page.getHeaders().getNodeType();

        KeyValueTuple result = page.readValues();
        this.keys = result.keys();
        this.pointers = result.valuePointers();
    }

    public Node(PageLoader pageLoader, Value[] keys, long[] pointers, byte type) {
        this.pageLoader = pageLoader;
        this.page = pageLoader.createPage(type);
        this.keys = keys;
        this.pointers = pointers;
        this.type = type;
    }


    public SplitResult addValue(Value key, long value) {
        int idx = Arrays.binarySearch(keys, key);

        if (page.getHeaders().getNodeType() == BRANCH) {
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            long nextPage = pointers[idx];
            SplitResult result = new Node(pageLoader, nextPage).addValue(key, value);
            if (result == null) {
                return null;
            }


            Value[] newKeys = new Value[keys.length + 1];
            System.arraycopy(keys, 0, newKeys, 0, idx);
            newKeys[idx] = result.key;
            System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);
            long[] newPointers = new long[pointers.length + 1];
            System.arraycopy(pointers, 0, newPointers, 0, idx + 1);
            newPointers[idx + 1] = result.right.page.getPageNumber();
            System.arraycopy(pointers, idx + 1, newPointers, idx + 2, pointers.length - idx - 1);

            this.keys = newKeys;
            this.pointers = newPointers;
            SplitResult sr = null;
            if (needsSplit(key)) {
                sr = splitBranch();
            }

            page.getHeaders().setPageSize(calculateSizeAfterInsert(key));
            page.getHeaders().setElemCount(keys.length);
            page.writeValues(keys, pointers);
            pageLoader.savePage(page);
            if (sr != null) {
                sr.right.page.writeValues(sr.right.keys, sr.right.pointers);
                pageLoader.savePage(sr.right.page);
            }

            return sr;

        } else if (page.getHeaders().getNodeType() == LEAF) {
            if (idx > -1) {
                pointers[idx] = value;
                return null;
            } else {
                idx = -(idx + 1);

                Value[] newKeys = new Value[keys.length + 1];
                System.arraycopy(keys, 0, newKeys, 0, idx);
                newKeys[idx] = key;
                System.arraycopy(keys, idx, newKeys, idx + 1, keys.length - idx);

                long[] newPointers = new long[keys.length + 1];
                System.arraycopy(pointers, 0, newPointers, 0, idx);
                newPointers[idx] = value;
                System.arraycopy(pointers, idx, newPointers, idx + 1, keys.length - idx);

                this.keys = newKeys;
                this.pointers = newPointers;
                SplitResult result = null;
                if (needsSplit(key)) {
                    result = splitLeaf();
                }

                page.writeValues(keys, pointers);
                pageLoader.savePage(page);

                if (result != null) {
                    result.right.page.writeValues(result.right.keys, result.right.pointers);
                    pageLoader.savePage(result.right.page);
                }

                return result;
            }


        } else {
            throw new UnsupportedOperationException();
        }
    }

    public long get(Value key) {
        int idx = Arrays.binarySearch(keys, key);

        if (page.getHeaders().getNodeType() == BRANCH) {
            idx = idx < 0 ? -(idx + 1) : idx + 1;
            long nextPage = pointers[idx];
            return new Node(pageLoader, nextPage).get(key);
        } else if (page.getHeaders().getNodeType() == LEAF) {
            if (idx > -1) {
                return pointers[idx];
            } else {
                return -1;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private boolean needsSplit(Value keyToAdd) {
        int currentSize = calculateSizeAfterInsert(keyToAdd);
        return currentSize > Page.PAGE_SIZE;
    }

    private int calculateSizeAfterInsert(Value keyToAdd) {
        return page.getHeaders().getPageSize() + Long.BYTES + Integer.BYTES + keyToAdd.getVal().length;
    }

    private SplitResult splitBranch() {
        int keyMid = keys.length / 2;
        int pointersMid = pointers.length / 2;
        Value promotedValue = keys[keyMid];

        Value[] leftKeys = new Value[keyMid];
        long[] leftPointers = new long[pointersMid];

        Value[] rightKeys = new Value[keys.length - keyMid - 1];
        long[] rightPointers = new long[pointers.length - pointersMid];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length);
        System.arraycopy(keys, keyMid + 1, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, pointersMid, rightPointers, 0, rightPointers.length);

        this.keys = leftKeys;
        this.pointers = leftPointers;
        Node rightNode = new Node(pageLoader, rightKeys, rightPointers, Node.BRANCH);
        return new SplitResult(promotedValue, this, rightNode);
    }

    private SplitResult splitLeaf() {
        int keyMid = keys.length / 2;
        int pointersMid = pointers.length / 2;

        Value[] leftKeys = new Value[keyMid];
        long[] leftPointers = new long[pointersMid];

        Value[] rightKeys = new Value[keys.length - keyMid];
        long[] rightPointers = new long[pointers.length - pointersMid];

        System.arraycopy(keys, 0, leftKeys, 0, leftKeys.length);
        System.arraycopy(pointers, 0, leftPointers, 0, leftPointers.length);
        System.arraycopy(keys, keyMid, rightKeys, 0, rightKeys.length);
        System.arraycopy(pointers, pointersMid, rightPointers, 0, rightPointers.length);

        this.keys = leftKeys;
        this.pointers = leftPointers;
        Node rightNode = new Node(pageLoader, rightKeys, rightPointers, Node.LEAF);
        return new SplitResult(rightNode.keys[0], this, rightNode);
    }

    static class SplitResult {
        Value key;
        Node left;
        Node right;

        public SplitResult(Value key, Node left, Node right) {
            this.key = key;
            this.left = left;
            this.right = right;
        }

        public SplitResult() {
        }
    }

}
