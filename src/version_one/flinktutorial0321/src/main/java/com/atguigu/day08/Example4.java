package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r -> r % 2)
                .reduce((r1, r2) -> r1 + r2).setParallelism(2)
                .print().setParallelism(2);

        env.execute();
    }
}
