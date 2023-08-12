package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                // a 1
                // a 2
                // ...
                .socketTextStream("localhost", 9999)
                // (a, 1000L)
                // (a, 2000L)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] array = in.split(" ");
                        return Tuple2.of(
                                array[0],
                                Long.parseLong(array[1]) * 1000L // 转换成毫秒单位
                        );
                    }
                })
                // 在map输出的数据流中插入水位线事件
                .assignTimestampsAndWatermarks(
                        // 将最大延迟时间设置为5秒钟
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 将f1字段指定为事件时间字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(in.f1 + 9999L);
                        out.collect("key: " + ctx.getCurrentKey() + "，当前process的水位线：" + ctx.timerService().currentWatermark() + "," +
                                "时间戳是：" + in.f1 + ",注册的定时器时间戳是：" + (in.f1 + 9999L));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key: " + ctx.getCurrentKey() + ",定时器时间戳：" + timestamp + ", 当前process的水位线：" +
                                "" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
