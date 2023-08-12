package com.atguigu.day03;

import com.atguigu.util.IntSource;
import com.atguigu.util.Statistic;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .process(new MyReduce())
                .print();

        env.execute();
    }

    public static class MyReduce extends KeyedProcessFunction<String, Integer, Statistic> {
        // 声明值状态变量
        private ValueState<Statistic> accumulator;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化值状态变量
            accumulator = getRuntimeContext().getState(
                    new ValueStateDescriptor<Statistic>(
                            "accmulator",
                            Types.POJO(Statistic.class)
                    )
            );
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<Statistic> out) throws Exception {
            // 第一条数据到来时，值状态变量为空
            // accumulator.value()访问的是输入数据的key所对应的状态变量
            if (accumulator.value() == null) {
                accumulator.update(new Statistic(in, in, in, 1, in));
            } else {
                Statistic oldAcc = accumulator.value();
                Statistic newAcc = new Statistic(
                        Math.min(in, oldAcc.min),
                        Math.max(in, oldAcc.max),
                        in + oldAcc.sum,
                        1 + oldAcc.count,
                        (in + oldAcc.sum) / (1 + oldAcc.count)
                );
                accumulator.update(newAcc);
            }

            out.collect(accumulator.value());
        }
    }
}
