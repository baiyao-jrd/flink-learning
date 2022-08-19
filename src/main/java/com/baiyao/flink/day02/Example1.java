package com.baiyao.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
                .fromElements("Hello world","Hello world")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = in.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word,1L));
                        }
                    }
                })
                .keyBy(r -> r.f0)
                .reduce((x,y) -> Tuple2.of(x.f0,x.f1+y.f1))
                .print();

        env.execute();
    }
}
