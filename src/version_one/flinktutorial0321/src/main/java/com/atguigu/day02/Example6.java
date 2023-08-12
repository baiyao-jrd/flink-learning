package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "black", "gray")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String in, Collector<String> out) throws Exception {
                        if (in.equals("white")) {
                            out.collect(in);
                        } else if (in.equals("black")) {
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print("匿名类");

        env
                .fromElements("white", "black", "gray")
                .flatMap(new MyFlatMap())
                .print("外部类");

        env
                .fromElements("white", "black", "gray")
                .flatMap((String in, Collector<String> out) -> {
                    if (in.equals("white")) {
                        out.collect(in);
                    } else if (in.equals("black")) {
                        out.collect(in);
                        out.collect(in);
                    }
                })
                // 类型擦除：Collector<String> -> Collector<Object>
                // 类型注解
                .returns(Types.STRING)
                .print("lambda");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String in, Collector<String> out) throws Exception {
            if (in.equals("white")) {
                out.collect(in);
            } else if (in.equals("black")) {
                out.collect(in);
                out.collect(in);
            }
        }
    }


}
