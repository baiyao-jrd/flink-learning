package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 单词计数
public class Example1 {
    // 不要忘记抛出异常！！！
    public static void main(String[] args) throws Exception {
        // 获取流执行环境（上下文）
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 定义处理数据的有向无环图（DAG）
        // 从socket读取数据
        // 先启动`nc -lk 9999`
        env
                .socketTextStream("localhost", 9999)
                // socketTextStream算子的并行子任务的数量是1
                .setParallelism(1)
                // map阶段："hello world" => ("hello", 1), ("world", 1)
                // 由于是1对多的转换，所以使用flatMap算子
                .flatMap(new Tokenizer())
                // flatMap算子的并行子任务的数量是1
                .setParallelism(1)
                // shuffle
                // 按照单词分组，将不同单词所对应的数据从逻辑上分开处理
                // KeySelector<输入数据的泛型, key的泛型>
                // keyby不做任何计算工作，所以不能设置并行子任务的数量
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> in) throws Exception {
                        // 将元组的f0字段指定为key，相当于scala中的`_1`
                        return in.f0;
                    }
                })
                // reduce阶段
                // in : ("hello", 1) -> acc : ("hello", 2) -> acc : ("hello", 3)
                // List : head::tail
                // reduce : tail.fold_left(head)((x,y) => max x y)
                .reduce(new WordCount())
                // reduce的并行子任务的数量是1
                .setParallelism(1)
                .print()
                // print算子的并行子任务的数量是1
                .setParallelism(1);

        // 提交并执行有向无环图
        env.execute();
    }

    // ReduceFunction<输入数据的泛型>
    // reduce的输入，输出和累加器的类型是一样的
    public static class WordCount implements ReduceFunction<Tuple2<String, Integer>> {
        // 两个参数：一个是输入数据，一个是累加器
        // 返回值是新的累加器
        // reduce函数定义了输入数据和累加器的聚合规则
        // 将累加器输出
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> input) throws Exception {
            return Tuple2.of(
                    input.f0,
                    accumulator.f1 + input.f1
            );
        }
    }

    // FlatMapFunction<INPUT, OUTPUT>
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 使用空格切分字符串
            String[] words = in.split(" ");
            // 将要发送的数据收集到集合中，由flink自动将数据发送出去
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
