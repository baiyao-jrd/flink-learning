package org.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 物理分区
*
* */
public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .shuffle()
                .print("随机发送")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .rebalance()
                .print("轮询发送")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .broadcast()
                .print("广播发送 - 最常用")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .global()
                .print("将数据发送到第一个并行子任务")
                .setParallelism(4);

        env.execute();
    }
}
