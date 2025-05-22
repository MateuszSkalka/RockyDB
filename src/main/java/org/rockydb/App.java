package org.rockydb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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

        int SIZE = 64 * 1024;
        Value[] toInsert = new Value[SIZE];
        for (int i = 0; i < SIZE; i++) {
            toInsert[i] = new Value((genRandomString() + "_" + i).getBytes());
            bLinkTree.addValue(toInsert[i], toInsert[i]);
        }

        int fails = 0;

        for (int i = 0; i < SIZE; i++) {
            var res = bLinkTree.get(toInsert[i]);
            if (res.compareTo(toInsert[i]) != 0) {
                System.out.println("Expected:  " + new String(toInsert[i].bytes()) + " Got:  " + new String(res.bytes()));
                fails++;
            }
        }
        System.out.println("N.O. ERRORS: " + fails);
    }
}
