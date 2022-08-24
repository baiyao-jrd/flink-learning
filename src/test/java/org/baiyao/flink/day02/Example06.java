package org.baiyao.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 使用flatmap实现过滤与复制
* */
public class Example06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("Red","Yellow","Blue")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String in, Collector<String> out) throws Exception {
                        if (in.equals("Red")) {
                            out.collect(in);
                        } else if (in.equals("Blue")) {
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print("使用匿名类方式实现过滤与复制");

        env
                .fromElements("Red","Yellow","Blue")
                .flatMap(new MyFlatMap())
                .print("使用外部类方式实现顾虑与复制");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String,String> {

        @Override
        public void flatMap(String in, Collector<String> out) throws Exception {
            if (in.equals("Red")) {
                out.collect(in);
            } else if (in.equals("Yellow")) {
                out.collect(in);
                out.collect(in);
            }
        }
    }
}
