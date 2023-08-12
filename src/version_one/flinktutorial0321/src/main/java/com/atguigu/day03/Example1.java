package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/// 物理分区算子
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .shuffle()
                .print("随机发送")
                .setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .rebalance()
                .print("rebalance")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .broadcast()
                .print("广播")
                .setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .global()
                .print("global")
                .setParallelism(8);

        env.execute();
    }
}
