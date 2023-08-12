package com.atguigu.day02;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// map举例
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent in) throws Exception {
                        return in.username;
                    }
                })
                .print("使用匿名类实现map");

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类实现map");

        env
                .addSource(new ClickSource())
                .map(r -> r.username)
                .print("使用匿名函数实现map");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<String> out) throws Exception {
                        out.collect(in.username);
                    }
                })
                .print("使用flatMap实现map");

        env
                .fromElements(1,2,3)
                .map(r -> Tuple2.of(r, r))
                // 类型擦除：Tuple2<Integer, Integer> -> Tuple2<Object, Object>
                .returns(Types.TUPLE(
                        Types.INT,
                        Types.INT
                ))
                .print();

        env.execute();
    }

    public static class MyMap implements MapFunction<ClickEvent, String> {
        @Override
        public String map(ClickEvent in) throws Exception {
            return in.username;
        }
    }
}
