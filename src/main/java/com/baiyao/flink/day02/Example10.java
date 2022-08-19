package com.baiyao.flink.day02;


import com.baiyao.flink.util.IntSource;
import com.baiyao.flink.util.Statistic;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 整型数据源 - 每来一条数据
*
* 计算历史数据的： 最大值、最小值、平均值、总和、共多少条数据
*
*
* 输出结果： min会越来越小，max会越来越大，sum越来越大，count越来越多，avg会慢慢收敛到500左右，因为咱们产生的随机值是0-1000
*           之内的数，等概率产生的话平均值会趋近于500 - 独立同分布
* */
public class Example10 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                //1. 整型过来之后先做一个基本转换
                .map(new MapFunction<Integer, Statistic>() {
                    @Override
                    public Statistic map(Integer in) throws Exception {
                        //2. 初始值的情况下，最小值、最大值、总和都是in，总条数是1，平均值也为in
                        return new Statistic(
                                in,
                                in,
                                in,
                                1,
                                in
                        );
                    }
                })
                //3. 每条数据过来之后，它的key都是"int"
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<Statistic>() {
                    @Override
                    //3. 输入数据和累加器的聚合规则
                    public Statistic reduce(Statistic acc, Statistic in) throws Exception {
                        return new Statistic(
                                Math.min(in.min, acc.min),
                                Math.max(in.max, acc.max),
                                in.sum + acc.sum,
                                1+acc.count,
                                (in.sum + acc.sum) / (1+acc.count)
                        );
                    }
                })
                .print();

        env.execute();
    }
}
