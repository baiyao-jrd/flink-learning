package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example3 {
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
                // 默认是200ms插入一次
                .assignTimestampsAndWatermarks(
                        // 将最大延迟时间设置为5秒钟
                        // forBoundedOutOfOrderness(Duration.ofSeconds(0)) 等价于 forMonotonousTimestamps()
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 将f1字段指定为事件时间字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new Trigger<Tuple2<String, Long>, TimeWindow>() {
                    // 每来一条数据 触发一次调用
                    @Override
                    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE; // 触发窗口计算，也就是触发下游的process算子的执行
                    }

                    // 处理时间定时器：当机器时间到达`time`参数时，触发执行
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    // 事件时间定时器：当水位线到达`time`参数时，触发执行
                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        if (time == window.getEnd() - 1L) {
                            return TriggerResult.FIRE_AND_PURGE; // 触发窗口计算并清空窗口
                        }
                        return TriggerResult.CONTINUE; // 什么都不做
                    }

                    // 当窗口闭合时触发执行
                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + "，窗口：" + context.window().getStart() + "~" +
                                "" + context.window().getEnd() + "，里面有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                })
                .print();

        env.execute();
    }
}
