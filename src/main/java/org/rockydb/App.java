package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class App {
    static String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    static Random random = new Random(1234L);

    public static String genRandomString(int min, int max) {
        int r = min + random.nextInt(max);
        char[] chars = new char[r];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = SALTCHARS.charAt(random.nextInt(SALTCHARS.length()));
        }
        return new String(chars);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Path path = Path.of("./super14.db");
        File file = path.toFile();

        NodeManager nodeManager = new NodeManager(file);
        BLinkTree bLinkTree = new BLinkTree(nodeManager);

        int SIZE = 256 * 1024;
        Value[] keys = new Value[SIZE];
        Value[] vals = new Value[SIZE];
        for (int i = 0; i < SIZE; i++) {
            keys[i] = new Value((genRandomString(8, 16) + "_" + i).getBytes());
            vals[i] = new Value((genRandomString(128, 256) + "_" + i).getBytes());
        }

        int fails = 0;

        int THREADS = 10;
        System.out.println("Running with " + THREADS + " threads");
        var cdl = new CountDownLatch(THREADS);
        var exService = Executors.newFixedThreadPool(THREADS);
        long start = System.currentTimeMillis();
        for (int i = 0; i < THREADS; i++) {
            int offset = i * SIZE / THREADS;
            exService.execute(() -> {
                for (int j = offset; j < Math.max(SIZE, offset + SIZE / THREADS); j++) {
                    bLinkTree.addValue(keys[j], vals[j]);
                }
                cdl.countDown();
            });
        }
        cdl.await();
        long end = System.currentTimeMillis();
        exService.shutdown();

        System.out.println("Time taken for writes: " + (end - start) + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            var res = bLinkTree.get(keys[i]);
            if (res == null) {
                System.out.println("Expected:  " + new String(vals[i].bytes()) + " Got:  null");
                fails++;
            } else if (res.compareTo(vals[i]) != 0) {
                System.out.println("Expected:  " + new String(vals[i].bytes()) + " Got:  " + new String(res.bytes()));
                fails++;
            }
        }
        end = System.currentTimeMillis();
        System.out.println("Time taken for reads: " + (end - start) + " ms");
        System.out.println("N.O. ERRORS: " + fails);
    }

}
