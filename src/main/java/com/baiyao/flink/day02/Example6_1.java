package com.baiyao.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * flatmap 【运行报错示例程序】
 *
 * 使用匿名函数编写出错详解
 *
 * 实现功能：
 *
 * 数据源【white、black、gray】
 * 将white原封不动输出、black复制一份、gray过滤掉
 *
 * */
public class Example6_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "black", "gray")
                //1. 输入输出均是字符串
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
                .fromElements("white","black","gray")
                .flatMap(new MyFlatMap())
                .print("外部类");

        env
                .fromElements("white","black","gray")
                .flatMap((in,out) -> {
                    if (in.equals("white")) {
                        out.collect(in);
                    } else if (in.equals("black")) {
                        out.collect(in);
                        out.collect(in);
                    }
                })
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
