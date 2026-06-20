package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    static String alphanumeric = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890";
    static Random random = new Random(47L);

    static int MIN_LEN = 1;
    static int MAX_LEN = 1028;

    static int THREADS = 16;

    static int INSERTS_PER_THREAD = 20_000;

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("starting generating random test set");
        var testSet = testSet();
        System.out.println("finished generating random test set");

        System.out.println("starting tree initialization");
        BLinkTree tree = new BLinkTree(new BufferedPool(
                new File("./database-" + UUID.randomUUID() + ".db"), 100_000
        ));
        System.out.println("finished tree initialization");

        ExecutorService es = Executors.newFixedThreadPool(THREADS);
        CountDownLatch insertsCdl = new CountDownLatch(THREADS);

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < testSet.size(); i++) {
            var tests = testSet.get(i);
            int finalI = i;
            es.submit(() -> {
                try {
                    for (var e : tests.entrySet()) {
                        tree.addValue(e.getKey(), e.getValue());
                    }
                    System.out.println("finished inserts for thread " + finalI);
                } catch (Exception exception) {
                    exception.printStackTrace();
                } finally {
                    insertsCdl.countDown();
                }
            });
        }
        insertsCdl.await();
        long t2 = System.currentTimeMillis();

        CountDownLatch validationCdl = new CountDownLatch(THREADS);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        long t3 = System.currentTimeMillis();
        for (int i = 0; i < testSet.size(); i++) {
            var tests = testSet.get(i);
            int finalI = i;
            es.submit(() -> {
                for (var e : tests.entrySet()) {
                    var res = tree.get(e.getKey());

                    if (e.getValue().equals(res)) {
                        successes.getAndIncrement();
                    } else {
                        failures.getAndIncrement();
                    }
                }
                System.out.println("finished inserts for thread " + finalI);
                validationCdl.countDown();
            });
        }
        validationCdl.await();
        long t4 = System.currentTimeMillis();

        System.out.println("Successes: " + successes.get());
        System.out.println("Failures: " + failures.get());
        System.out.println("Insertion time: " + (t2 - t1));
        System.out.println("Validation time: " + (t4 - t3));

        es.shutdown();
    }

    static List<Map<Value, Value>> testSet() {
        List<Map<Value, Value>> testSet = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            Map<Value, Value> map = new HashMap<>();
            for (int j = 0; j < INSERTS_PER_THREAD; j++) {
                Value val = random();
                map.put(val, val);
            }
            testSet.add(map);
        }
        return testSet;
    }

    static Value random() {
        int len = random.nextInt(MIN_LEN, MAX_LEN);
        var sb = new StringBuilder();

        for (int i = 0; i < len; i++) {
            sb.append(randomAlphanumeric());
        }
        return new Value(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    static char randomAlphanumeric() {
        return alphanumeric.charAt(random.nextInt(alphanumeric.length()));
    }

}
