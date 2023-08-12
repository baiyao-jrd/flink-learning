package com.atguigu.day02;

import com.atguigu.util.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
