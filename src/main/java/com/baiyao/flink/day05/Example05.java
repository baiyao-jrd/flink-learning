package com.baiyao.flink.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream(
                "hadoop102",
                7777)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String in) throws Exception {
                        String[] strings = in.split(" ");
                        return Tuple2.of(
                                strings[0],
                                Long.parseLong(strings[1]) * 1000L
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {

                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .process(
                        new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Long> in, Context ctx, Collector<String> out) throws Exception {
                                ctx.timerService().registerProcessingTimeTimer(in.f1+9999L);
                                out.collect("当前key： " + ctx.getCurrentKey() + " "
                                        + "水位线： " + ctx.timerService().currentWatermark() + " "
                                        + "发生时间： " + in.f1 + " "
                                        + "定时器时间： " + (in.f1+9999L)
                                );
                            }

                            @Override
                            public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect(
                                        "当前key: " + ctx.getCurrentKey() + " "
                                        + "水位线： " + ctx.timerService().currentWatermark() + ""
                                        + "定时器时间： " + ts
                                );
                            }
                        }
                )
                .print();

        env.execute();
    }
}
