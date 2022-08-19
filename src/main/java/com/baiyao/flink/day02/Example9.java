package com.baiyao.flink.day02;

import org.apache.flink.util.MathUtils;

/*
 * key是如何决定要去哪个并行子任务的？？
 *
 * 下面是flink底层依赖的计算方式
 * */
public class Example9 {
    public static void main(String[] args) {
        //1. key是1
        Integer key = 1;
        //2. 获取key的hashCode
        int hashCode = key.hashCode();
        //3. 计算key hashCode的murmurHash值
        int murmurHash = MathUtils.murmurHash(hashCode);
        //4. 接下来计算这个key要去哪一个并行子任务，设置默认最大并行度为128，reduce的并行度为4
        int idx = (murmurHash % 128) * 4 / 128;
        //5. 获取reduce并行子任务的索引
        System.out.println(idx);

    }
}
