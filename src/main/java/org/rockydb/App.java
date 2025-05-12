package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class App {
    static String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    static Random random = new Random(1234L);

    public static String genRandomString() {
        int r = 8 + random.nextInt(8);
        char[] chars = new char[r];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = SALTCHARS.charAt(random.nextInt(SALTCHARS.length()));
        }
        return new String(chars);
    }

    public static void main(String[] args) throws IOException {
        Path path = Path.of("./super.db");
        File file = path.toFile();

        NodeManager nodeManager = new NodeManager(file);
        BLinkTree bLinkTree = new BLinkTree(nodeManager);

        int SIZE = 2 * 1024;
        Map<Value, Long> map = new HashMap<>();
        for (int i = 0; i < SIZE; i++) {
            map.put(new Value((genRandomString() + i).getBytes()), (long) i);
        }

        for (var e : map.entrySet()) {
            bLinkTree.addValue(e.getKey(), e.getValue());
        }

        int fails = 0;
        long start = System.currentTimeMillis();
        for (var e : map.entrySet()) {
            var r = bLinkTree.get(e.getKey());
            if (r != e.getValue()) {
                System.out.println("Expected = " + e.getValue() + " got = " + r);
            }
        }
        long stop = System.currentTimeMillis();

        System.out.println("Read " + SIZE + " values in: " + (stop - start) + " millis");
        System.out.println("FAILED:  " + fails);

    }
}
