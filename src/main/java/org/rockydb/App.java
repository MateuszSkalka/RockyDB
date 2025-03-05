package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class App {
    static String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    static Random random = new Random(1234L);

    public static String genRandomString() {
        int r = random.nextInt(500);
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
        if (!nodeManager.anyNodeExists()){
            Node root = nodeManager.writeNode(Node.LEAF, new Value[]{}, new long[]{});
        }

        int SIZE = 32 * 1024;
        Node root = nodeManager.readNode(1L);
        Object[][] kkk = new Object[SIZE][2];
        for (int i = 0; i < SIZE; i++) {
            kkk[i][0] = new Value((genRandomString()).getBytes());
            kkk[i][1] = (long) i;
        }
        List<Object[]> suf = new ArrayList<>(List.of(kkk));

        Collections.shuffle(suf, new Random(123L));

        for (int i = 0; i < SIZE; i++) {
            Object[] o = suf.get(i);
            var res = root.addValue((Value) o[0], (long) o[1]);
            if (res != null) {
                root = nodeManager.writeNode(Node.BRANCH, new Value[]{res.key}, new long[]{res.left.getId(), res.right.getId()});
            }
        }


        long start = System.currentTimeMillis();
        for (int i = 0; i < SIZE; i++) {
            Object[] o = suf.get(i);
            var resp = root.get((Value) o[0]);
            System.out.println("For key: " + new String(((Value) o[0]).val()) + " the value is " + resp);
        }
        long stop = System.currentTimeMillis();

        System.out.println("Read " + SIZE +  " values in: " + (stop - start) + " millis");

        System.out.println("root id : " + root.getId());

    }
}
