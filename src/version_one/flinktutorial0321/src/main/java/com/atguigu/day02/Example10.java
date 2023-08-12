package com.atguigu.day02;

import com.atguigu.util.IntSource;
import com.atguigu.util.Statistic;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
//                .fromElements(1,10)
                .addSource(new IntSource())
                .map(new MapFunction<Integer, Statistic>() {
                    @Override
                    public Statistic map(Integer in) throws Exception {
                        return new Statistic(
                                in,
                                in,
                                in,
                                1,
                                in
                        );
                    }
                })
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<Statistic>() {
                    @Override
                    public Statistic reduce(Statistic acc, Statistic in) throws Exception {
                        return new Statistic(
                                Math.min(in.min, acc.min),
                                Math.max(in.max, acc.max),
                                in.sum + acc.sum,
                                1 + acc.count,
                                (in.sum + acc.sum) / (1 + acc.count)
                        );
                    }
                })
                .print();

        env.execute();
    }
}
