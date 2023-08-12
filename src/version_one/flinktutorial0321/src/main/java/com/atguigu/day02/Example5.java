package com.atguigu.day02;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// filter举例
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent in) throws Exception {
                        return in.username.equals("Mary");
                    }
                })
                .print("匿名类");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("外部类");

        env
                .addSource(new ClickSource())
                .filter(r -> r.username.equals("Mary"))
                .print("lambda");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<ClickEvent> out) throws Exception {
                        if (in.username.equals("Mary")) out.collect(in);
                    }
                })
                .print("flatMap");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<ClickEvent> {
        @Override
        public boolean filter(ClickEvent in) throws Exception {
            return in.username.equals("Mary");
        }
    }
}
