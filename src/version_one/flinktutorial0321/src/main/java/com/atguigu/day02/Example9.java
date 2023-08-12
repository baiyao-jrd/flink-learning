package com.atguigu.day02;

import org.apache.flink.util.MathUtils;

public class Example9 {
    public static void main(String[] args) {
        // key是1
        Integer key = 0;
        // 获取key的hashCode
        int hashCode = key.hashCode();
        // 计算key的hashCode的murmurHash值
        int murmurHash = MathUtils.murmurHash(hashCode);
        // 设置默认的最大并行度是128
        // reduce的并行度是4
        int idx = (murmurHash % 128) * 4 / 128;
        System.out.println(idx);
    }
}
