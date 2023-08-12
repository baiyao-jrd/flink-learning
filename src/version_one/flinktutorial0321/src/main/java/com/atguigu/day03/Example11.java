package com.atguigu.day03;

import com.atguigu.util.IntSource;
import com.atguigu.util.Statistic;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return "int";
                    }
                })
                .process(new MyReduce())
                .print();

        env.execute();
    }

    public static class MyReduce extends KeyedProcessFunction<String, Integer, Statistic> {
        // 声明值状态变量
        private ValueState<Statistic> accumulator;
        // 标志位，用来指示是否存在定时器
        private ValueState<Integer> flag;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化值状态变量
            accumulator = getRuntimeContext().getState(
                    new ValueStateDescriptor<Statistic>(
                            "accmulator",
                            Types.POJO(Statistic.class)
                    )
            );
            flag = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>(
                            "flag",
                            Types.INT
                    )
            );
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<Statistic> out) throws Exception {
            // 第一条数据到来时，值状态变量为空
            // accumulator.value()访问的是输入数据的key所对应的状态变量
            // ctx.getCurrentKey()
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

            // 如果没有定时器存在，那么注册一个定时器
            if (flag.value() == null) {
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + 10 * 1000L
                );
                flag.update(1);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Statistic> out) throws Exception {
            // ctx.getCurrentKey()
            out.collect(accumulator.value());
            // 发送完统计结果之后，将标志位清空
            flag.clear();
        }
    }
}
