package com.baiyao.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 * 单词计数案例 - 从文件读取数据
 *
 * words.txt内容：
 *
 * Hello world
 * baiyao org
 * Hello world
 * baiyao rundong
 *
 * 输出结果：
 *
 * (Hello,1)
 * (world,1)
 * (baiyao,1)
 * (org,1)
 * (Hello,2)
 * (world,2)
 * (baiyao,2)
 * (rundong,1)
 *
 *
 * */
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //对每一个算子设置并行子任务比较麻烦这里进行统一设置，设置为1
        env.setParallelism(1);

        env
                .readTextFile("E:\\flinkcode\\flink-learning\\src\\main\\resources\\words.txt")
                //注意： 这里要使用flink的二元组，java是面向对象语言，并没有原生的对二元组的支持
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = in.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word,1));
                        }
                    }
                })
                //使用箭头函数，左侧为输入数据右侧为输出数据，将二元组的第一个字段指定为key
                .keyBy(w -> w.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        return Tuple2.of(
                                acc.f0,
                                acc.f1 + in.f1
                        );
                    }
                })
                .print();

        env.execute();
    }
}
