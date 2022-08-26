package org.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/*
* 使用processfunction实现flatmap的功能
* */
public class Example07 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white","black","gray")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (in.equals("white")) {
                            out.collect(in);
                        } else if (in.equals("black")) {
                            out.collect(in);
                            out.collect(in);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
