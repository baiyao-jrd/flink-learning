package org.baiyao.flink.day02;

import com.baiyao.flink.util.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .print("main");

        env.execute();
    }
}
