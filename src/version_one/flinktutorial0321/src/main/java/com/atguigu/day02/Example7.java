package com.atguigu.day02;

public class Example7 {
    public static void main(String[] args) {
        System.out.println(add(1,2));
        System.out.println(add("x","y"));
        System.out.println(add(1, "x"));
    }
    public static Object add(Object x, Object y) {
        if (x instanceof Integer && y instanceof Integer) {
            return (int)x + (int)y;
        } else if (x instanceof String && y instanceof String) {
            return x.toString() + y.toString();
        } else {
            throw new RuntimeException("类型不匹配！");
        }
    }
}
