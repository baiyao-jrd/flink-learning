package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
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
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 将f1字段指定为事件时间字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 将迟到且对应窗口已经销毁的数据，发送到侧输出流中
                .sideOutputLateData(
                        // 泛型和窗口中的元素类型一致
                        new OutputTag<Tuple2<String, Long>>("late-event") {
                        }
                )
                // 等待迟到数据5秒钟
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + "，窗口：" + context.window().getStart() + "~" +
                                "" + context.window().getEnd() + "，里面有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                });

        result.print("main");
        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late-event"){}).print("side");

        env.execute();
    }
}
