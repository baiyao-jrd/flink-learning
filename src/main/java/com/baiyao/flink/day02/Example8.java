package com.baiyao.flink.day02;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example8 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .keyBy(i -> i % 3)
                .reduce((acc,num) -> acc+num)
                .setParallelism(4)
                .print()
                .setParallelism(4);
        env.execute();
    }
}
