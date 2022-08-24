package org.baiyao.flink.day02;

import com.baiyao.flink.util.ClickEvent;
import com.baiyao.flink.util.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 实现map调取输出mojo类的某一个属性
*
* */
public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .map(r -> r.username)
                .print("使用匿名函数的方式");

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("使用外部类的方式");

        env
                .addSource(new ClickSource())
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent in) throws Exception {
                        return in.username;
                    }
                })
                .print("使用匿名类的方式");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<String> out) throws Exception {
                        out.collect(in.username);
                    }
                })
                .print("使用flatmap实现map");

        env.execute();
    }

    public static class MyMap implements MapFunction<ClickEvent,String> {

        @Override
        public String map(ClickEvent in) throws Exception {
            return in.username;
        }
    }
}
