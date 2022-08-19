package com.baiyao.flink.day02;


/*
* 实现字符串与整数的加法
*
* 同一个函数既可以实现对两个字符串的加法，也可以实现两个整数的加法
*
* 不用泛型实现 - java1.5之前
* */
public class Example7 {

    //这种代码的抽象程度不高，引入了泛型，但是这种代码性能好
    public static Object add(Object x, Object y) {
        if (x instanceof Integer && y instanceof Integer) {
            return (int)x + (int)y;
        } else if (x instanceof String && y instanceof String) {
            return x.toString() + y.toString();
        } else {
            throw new RuntimeException("类型不匹配！");
        }
    }

    public static void main(String[] args) {
        System.out.println(add(1, 2));
        System.out.println(add("x", "y"));
        System.out.println(add(1, "x"));
    }
}
