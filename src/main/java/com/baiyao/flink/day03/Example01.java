package com.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


//物理分区算子
//将数据路由到下游算子的不同的并行子任务。
//
//shuffle()：随机向下游的并行子任务发送数据。这里的shuffle和之前keyBy的shuffle不是一回事儿！
//rebalance()：将数据轮询发送到下游的所有并行子任务中。round-robin。
//rescale()：将数据轮询发送到下游的部分并行子任务中。用在下游算子的并行度是上游算子的并行度的整数倍的情况。round-robin。
//broadcast()：将数据广播到下游的所有并行子任务中。
//global()：将数据发送到下游的第一个（索引为0）并行子任务中。
//custom()：自定义分区。可以自定义将某个key的数据发送到下游的哪一个并行子任务中去。

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
