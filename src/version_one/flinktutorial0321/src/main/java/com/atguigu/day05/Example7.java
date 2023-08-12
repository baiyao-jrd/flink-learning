package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example7 {
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
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 告诉flink事件时间是哪一个字段
                                    }
                                };
                            }

                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    private long delay = 5000L; // 最大延迟时间
                                    // 防止溢出，加上`delay+1L`
                                    private long maxTs = Long.MIN_VALUE + delay + 1L; // 用来保存观察到的最大事件时间
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        // onEvent每来一条数据调用一次
                                        maxTs = Math.max(maxTs, event.f1);
                                        // 如果数据的key是hello，发送一条正无穷大的水位线
                                        if (event.f0.equals("hello")) {
                                            output.emitWatermark(new Watermark(Long.MAX_VALUE));
                                        }
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 周期性调用，默认每隔200ms调用一次
                                        // 向下游发送水位线事件
                                        output.emitWatermark(new Watermark(maxTs - delay - 1L));
                                    }
                                };
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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
