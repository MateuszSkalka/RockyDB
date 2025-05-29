package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import static org.rockydb.ByteUtils.readIsLeafFlag;
import static org.rockydb.ByteUtils.readIsLeftmostNodeFlag;

public class DiscStore implements AutoCloseable, Store {
    public static final int PAGE_SIZE = 8 * 1024;
    public static final int PAGE_HEADERS_SIZE = 5;
    public static final int KEY_PREFIX_SIZE = 4;
    public static final int VALUE_POINTER_SIZE = 8;
    private static final long TREE_ROOT_FILE_POSITION = 0;

    private final RandomAccessFile raf;
    private final FileChannel fileChannel;
    private final StripedLock stripedLock;
    private final AtomicLong nextPageId;
    private final AtomicLong rootId;

    public DiscStore(File dbFile) throws IOException {
        raf = new RandomAccessFile(dbFile, "rw");
        this.fileChannel = raf.getChannel();
        this.stripedLock = new StripedLock();
        this.nextPageId = new AtomicLong(loadNextPageId());
        this.rootId = new AtomicLong(loadRootId());
        checkAndInitTree();
    }

    @Override
    public Node writeNode(Node node) {
        ByteBuffer buffer;
        if (node instanceof BranchNode branchNode) {
            buffer = createBuffer(node.isLeaf(), node.isLeftmostNode(), node.height(), branchNode.getKeys(), branchNode.getPointers());
        } else if (node instanceof LeafNode leafNode) {
            buffer = createBuffer(node.isLeaf(), node.isLeftmostNode(), node.height(), leafNode.getKeys(), leafNode.getValues());
        } else {
            throw new IllegalArgumentException();
        }

        long nodeId = node.id() == null ? nextPageId.getAndIncrement() : node.id();
        buffer.rewind();
        stripedLock.runInWriteLock(nodeId, () -> {
            try {
                fileChannel.write(buffer, PAGE_SIZE * nodeId);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        node.setId(nodeId);
        return node;
    }

    @Override
    public Node readNode(long id) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        stripedLock.runInReadLock(id, () -> {
            try {
                fileChannel.read(buffer, id * PAGE_SIZE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        buffer.rewind();

        byte flags = buffer.get();
        boolean isLeaf = readIsLeafFlag(flags);
        boolean isLeftmostNode = readIsLeftmostNodeFlag(flags);
        int elemCount = buffer.getShort();
        int height = buffer.getShort();

        if (isLeaf) {
            return readLeafNode(id, isLeftmostNode, height, buffer, elemCount);
        } else {
            return readBranchNode(id, isLeftmostNode, height, buffer, elemCount);
        }
    }

    @Override
    public void updateRootId(long id) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        buffer.putLong(id);
        buffer.rewind();
        try {
            fileChannel.write(buffer, TREE_ROOT_FILE_POSITION);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rootId.set(id);
    }

    @Override
    public long rootId() {
        return rootId.get();
    }

    private LeafNode readLeafNode(long id, boolean isLeftmost, int height, ByteBuffer buffer, int elemCount) {
        Value[] keys = readValueArray(buffer, elemCount);
        Value[] values = readValueArray(buffer, elemCount + 1);
        return new LeafNode(id, isLeftmost, height, keys, values);
    }

    private BranchNode readBranchNode(long id, boolean isLeftmost, int height, ByteBuffer buffer, int elemCount) {
        Value[] keys = readValueArray(buffer, elemCount);
        long[] values = readLongArray(buffer, elemCount + 1);
        return new BranchNode(id, isLeftmost, height, keys, values);
    }

    private Value[] readValueArray(ByteBuffer buffer, int size) {
        Value[] arr = new Value[size];
        for (int i = 0; i < arr.length; i++) {
            int nextSize = buffer.getInt();
            byte[] bytes = new byte[nextSize];
            buffer.get(bytes);
            arr[i] = new Value(bytes);
        }
        return arr;
    }

    private long[] readLongArray(ByteBuffer buffer, int size) {
        long[] arr = new long[size];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = buffer.getLong();
        }
        return arr;
    }


    private ByteBuffer createBuffer(boolean isLeaf, boolean isLeftmost, int height, Value[] keys, long[] pointers) {
        ByteBuffer buffer = createBuffer(isLeaf, isLeftmost, keys.length, height);

        for (Value key : keys) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        for (long valuePointer : pointers) {
            buffer.putLong(valuePointer);
        }
        return buffer;
    }

    private ByteBuffer createBuffer(boolean isLeaf, boolean isLeftmost, int height, Value[] keys, Value[] values) {
        ByteBuffer buffer = createBuffer(isLeaf, isLeftmost, keys.length, height);

        for (Value key : keys) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        for (Value key : values) {
            buffer.putInt(key.bytes().length);
            buffer.put(key.bytes());
        }
        return buffer;
    }

    private ByteBuffer createBuffer(boolean isLeaf, boolean isLeftmost, int numOfKeys, int height) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[PAGE_SIZE]);
        // is leaf flag
        buffer.put(ByteUtils.createFlags(isLeaf, isLeftmost));
        //number of keys
        buffer.putShort((short) numOfKeys);
        // height
        buffer.putShort((short) height);
        return buffer;
    }

    private long loadNextPageId() throws IOException {
        return Math.max(1, raf.length() / PAGE_SIZE);
    }

    private long loadRootId() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        fileChannel.read(buffer, TREE_ROOT_FILE_POSITION);
        buffer.rewind();
        long val = buffer.getLong();
        if (val < 1) return -1;
        else return val;
    }

    private void checkAndInitTree() {
        if (rootId.get() == -1) {
            Node root = writeNode(new LeafNode(null, true, 1, new Value[]{}, new Value[]{new Value(ByteUtils.toByteArray(-1L))}));
            updateRootId(root.id());
        }
    }

    @Override
    public void close() throws Exception {
        if (fileChannel != null) {
            fileChannel.close();
        }
        if (raf != null) {
            raf.close();
        }
    }
}
