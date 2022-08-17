package com.baiyao.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 *
 * 单词计数案例
 *
 * */
public class Example1 {

    //抛出异常 - 调用的某些API需要抛出异常，方便起见，直接在main函数这里抛出异常
    public static void main(String[] args) throws Exception {
        //1. 获取流执行环境上下文【程序代码的作用域】
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 定义处理数据的有向无环图（DAG）- 为将DAG打包成jar包提交到集群上面处理数据做铺垫
        env
                //2.1 先在虚拟机中启动nc -lk 9999，从socket读取数据，其输出数据是string类型
                .socketTextStream("hadoop102",9999)
                //2.2 设置socketTextStream算子的并行子任务数量为1
                .setParallelism(1)
                //2.3 map阶段： "Hello world" => ("Hello",1),("world",1)
                //              按照空格将字符串切分，然后针对每一个字符串标记一个1，底层就是转换成了二元组，这是一对多的转换，使用flatmap算子
                //              flatmap作用： 将数据流或者列表里面的每一个元素转换成0个、1个、或者多个元素
                //              Tokenizer类： 定义了flatmap的具体逻辑 => socket数据接收过来进行扁平化操作，扁平化的具体逻辑在Tokenizer里面声明
                .flatMap(new Tokenizer())
                //2.4 设置flatmap算子的并行子任务数量为1
                .setParallelism(1)
                //2.5 打印
                .print()
                //2.6 设置print算子的并行子任务数量为1
                .setParallelism(1);

        //3. 提交并执行定义的有向无环图
        env.execute();
    }

    // 2.3.1 Tokenizer实现FlatMapFunction接口，这个接口有两个泛型： ① socket输出的泛型String ② flatmap转换完之后一对多输出的泛型二元组
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        //2.3.1.1 实现接口的flatmap方法： 输入数据字符串，输出数据为集合 - 集合收集的数据会向下游发送，flink会自动的把数据发送出去
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            //2.3.1.1.1 使用空格切分字符串
            String[] words = in.split(" ");
            //2.3.1.1.2 集合收集数据，由flink将数据自动发出去，Tuple2.of()实例化一个二元组
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
