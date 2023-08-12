package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .keyBy(r -> r % 3)
                .reduce((v1, v2) -> v1 + v2)
                .setParallelism(4)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
