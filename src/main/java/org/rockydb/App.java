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

    public static String genRandomString() {
        int r = 8 + random.nextInt(8);
        char[] chars = new char[r];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = SALTCHARS.charAt(random.nextInt(SALTCHARS.length()));
        }
        return new String(chars);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Path path = Path.of("./super.db");
        File file = path.toFile();

        NodeManager nodeManager = new NodeManager(file);
        BLinkTree bLinkTree = new BLinkTree(nodeManager);

        int SIZE = 256 * 1024;
        Value[] toInsert = new Value[SIZE];
        for (int i = 0; i < SIZE; i++) {
            toInsert[i] = new Value((genRandomString() + "_" + i).getBytes());
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
                    bLinkTree.addValue(toInsert[j], toInsert[j]);
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
            var res = bLinkTree.get(toInsert[i]);
            if (res == null) {
                System.out.println("Expected:  " + new String(toInsert[i].bytes()) + " Got:  null");
                fails++;
            } else if (res.compareTo(toInsert[i]) != 0) {
                System.out.println("Expected:  " + new String(toInsert[i].bytes()) + " Got:  " + new String(res.bytes()));
                fails++;
            }
        }
        end = System.currentTimeMillis();
        System.out.println("Time taken for reads: " + (end - start) + " ms");
        System.out.println("N.O. ERRORS: " + fails);
    }

}
