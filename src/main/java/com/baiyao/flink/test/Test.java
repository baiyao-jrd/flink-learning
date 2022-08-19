package com.baiyao.flink.test;

import java.util.Random;

public class Test {
    public static void main(String[] args) throws Exception{
        String[] user = {"a","b","c"};

        while (true) {
            System.out.println(user[new Random().nextInt(user.length)]);
            Thread.sleep(1000L);
        }

    }

}
