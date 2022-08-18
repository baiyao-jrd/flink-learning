package com.baiyao.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
 *
 * 单词计数案例【从socket消费数据】
 *
 * 过程总结：
 * 输入Hello world
 * 经过flatmap之后变成("Hello",1),("world",1)
 * ("Hello",1)经过keyby之后分到了Hello对应的小组
 * ("world",1)经过keyby之后分到了world对应的小组
 * ("Hello",1)到reduce之后，由于是Hello对应的小组的第一条数据，所以会直接作为累加器输出，打印出来的就是一个二元组("Hello",1)
 * ("world",1)到reduce之后，由于是world对应的小组的第一条数据，所以会直接作为累加器输出，打印出来的就是一个二元组("world",1)
 *
 * 再输入Hello world
 * 经过flatmap之后变成("Hello",1),("world",1)
 * ("Hello",1)经过keyby之后分到了Hello对应的小组，("Hello",1)到reduce之后，由于是Hello对应的小组累加器为("Hello",1)，
 *             所以输入的("Hello",1)会与累加器("Hello",1)做聚合累加，变成("Hello",2)打印出来
 * ("world",1)经过keyby之后分到了world对应的小组，("world",1)到reduce之后，由于是world对应的小组累加器为("world",1)，
 *             所以输入的("world",1)会与累加器("world",1)做聚合累加，变成("world",2)打印出来
 *
 * 【注意】
 * 1. 累加器没有办法设置初始值，foldLeft可以 - 折叠操作
 * 2. flatmap是无状态算子
 * 3. reduce是有状态算子，需要一个累加器来维护内部状态，进行聚合操作
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
                //              Tokenizer类： 定义了flatmap的具体逻辑 => socket数据接收过来进行扁平化操作，
                //                            扁平化的具体逻辑在Tokenizer里面声明
                .flatMap(new Tokenizer())
                //2.4 设置flatmap算子的并行子任务数量为1
                .setParallelism(1)
                //2.5 【shuffle阶段】: 按照单词来进行分组，key逻辑分组，使用匿名类实现shuffle的逻辑
                //2.5.1 new KeySelector<输入数据的泛型，key的泛型>
                //      keyby作用： 来一条数据，给这条数据指定一个key，将其路由到下游的任务中，keyby不做计算工作不用设置并行子任务数量，
                //      他不是算子只用来路由数据，flatmap有转换作用，keyby没有
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> in) throws Exception {
                        //2.5.2 将元组的f0字段指定为key，就是元组的第一个字段，相当于scala中的`_1`
                        return in.f0;
                    }
                })
                //2.6 【reduce阶段】： in: ("Hello",1) -> acc: ("Hello",2) -> acc: ("Hello",3)
                //                    刚开始累加器是空的，第一条数据来了之后累加器是空的他会直接把第一条数据作为累加器保存然后输出，
                //                    后面的数据来了之后输入数据会和累加器做聚合形成新的累加器，接着把累加器输出
                //                    reduce的输入输出类型和累加器类型一样
                //                    注意： reduce定义的是输入数据和累加器的聚合规则，返回的仍是累加器
                .reduce(new WordCount())
                //2.7 设置reduce算子的并行子任务数量为1

                /*
                * reduce只有一个并行子任务(跑在了一个线程里面)，那么它如何把不同小组(Hello，1) (world,1)的数据区分开进行分组处理的呢？
                *
                * 使用HashMap，数据里面的key就是hash表里面的key
                * 1. (Hello,1)到来reduce之后，首先会在hash表里面查找key为Hello的累加器，没有找到，
                *    (Hello,1)就会作为累加器保存下来，其实就是put了一个key，value键值对，key是Hello，
                *    value就是(Hello,1)，经历算子print()，就会将(Hello,1)输出
                * 2. (world,1)到来reduce之后，首先会在hash表里面查找key为world的累加器，没有找到，
                *    (world,1)就会作为累加器保存下来，其实就是put了一个key，value键值对，key是world，
                *    value就是(world,1)，经历算子print()，就会将(world,1)输出
                * 3. 继续输入Hello world
                * 4. (Hello,1)到来reduce之后，首先会在hash表里面查找key为Hello的累加器，找到，与(Hello,1)
                *    做聚合得到(Hello,2)，原来累加器的值(Hello,1)会被删掉，接着历经print()输出(Hello,2)
                * 5. (world,1)同理
                *
                * reduce并行子任务维护内部状态的方式就是使用hashmap，如果设置了保存检查点，就可以把hash表隔一段时间
                * 保存到硬盘上，对于每一个key，reduce维护一个累加器
                *
                * */
                .setParallelism(1)
                //2.x 打印
                .print()
                //2.x 设置print算子的并行子任务数量为1
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

    //2.6.1 ReduceFunction<>只有一个泛型，输入泛型 - 一个二元组，flatmap的输出类型是一个二元组，
    //      经过keyby按照key原封不动的将二元组路由到不同的reduce，
    //      reduce的输入和输出类型与累加器的类型是一样的，所以只有一个输入泛型
    public static class WordCount implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        //2.6.2 两个参数： 一个是输入数据，一个是累加器，reduce函数定义输入数据和累加器聚合规则，返回值是新的累加器
        //                ，输入数据和累加器做聚合产生新的累加器覆盖旧的累加器
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> input) throws Exception {
            //  返回新的累加器
            return Tuple2.of(
                    //单词key保留
                    input.f0,
                    //聚合累加器： 元组的第二个值相加
                    accumulator.f1 + input.f1
            );
        }
    }
}
