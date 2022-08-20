package com.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example01 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1. fromelements并行度必须设置为1，单并行度数据源
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
                .print("reblance")
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
