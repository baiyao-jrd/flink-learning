package com.baiyao.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * flatmap 【运行报错示例程序 - 解释】
 *
 * 使用匿名函数编写出错详解
 *
 * 实现功能：
 *
 * 数据源【white、black、gray】
 * 将white原封不动输出、black复制一份、gray过滤掉
 *
 * */
public class Example6_2 {
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

        /*
        * lambda表达式 - 类型擦除
        * */
        env
                .fromElements("white","black","gray")
                //.flatMap((String in, Collector<String> out) -> {   即便标注上类型，执行依然会报错

                /*
                * scala中会进行类型推断(in,out)，能够推断出in的类型为String，out的类型为String的集合
                * java推断不出来，因为java在编译字节码时存在类型擦除的机制
                *
                * Collector<String> 会被类型擦除为 Collector<Object> 所以需要进行类型注解
                *
                *
                * */
                .flatMap((String in,Collector<String> out) -> {
                    if (in.equals("white")) {
                        out.collect(in);
                    } else if (in.equals("black")) {
                        out.collect(in);
                        out.collect(in);
                    }
                })
                //类型注解 - 告诉flink，flatmap输出的类型是什么，输入类型不用告知
                //          输入数据in类型为String，也就是fromElements的输出类型
                //          输出类型out不为基本数据类型，其为String集合

                //          这里为输出的集合里面的数据类型做注解
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
