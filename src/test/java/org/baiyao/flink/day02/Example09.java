package org.baiyao.flink.day02;

import org.apache.flink.util.MathUtils;

/*
* 计算key为2的数据要去的reduce的并行子任务的索引值【keyBy的底层原理】
* */
public class Example09 {
    public static void main(String[] args) {
        Integer key = 2;
        int index = (MathUtils.murmurHash(key.hashCode()) % 128) * 4 / 128;
        System.out.println(index);

        //reduce算子的并行度为4，第三个并行子任务的索引是2，128是默认的最大并行度
    }
}
