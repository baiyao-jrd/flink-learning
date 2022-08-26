package com.baiyao.flink.day07;

import com.baiyao.flink.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("E:\\flinkcode\\flink-learning\\src\\main\\resources\\UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userbehavior = new UserBehavior(
                                array[0],
                                array[1],
                                array[2],
                                array[3],
                                Long.parseLong(array[4]) * 1000L
                        );

                        if (userbehavior.type.equals("pv")) {
                            out.collect(userbehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<UserBehavior>() {
                                            @Override
                                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                                return element.ts;
                                            }
                                        }
                                )
                )
                .keyBy(r -> "userbehavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, HashSet<String>, Long>() {
                            @Override
                            public HashSet<String> createAccumulator() {
                                return new HashSet<>();
                            }

                            @Override
                            public HashSet<String> add(UserBehavior in, HashSet<String> acc) {
                                acc.add(in.userId);
                                return acc;
                            }

                            @Override
                            public Long getResult(HashSet<String> acc) {
                                return (long) acc.size();
                            }

                            @Override
                            public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {

                            @Override
                            public void process(String key, Context ctx, Iterable<Long> in, Collector<String> out) throws Exception {
                                System.out.println("窗口： " + ctx.window().getStart() + "~" + ctx.window().getEnd() + " , " + "uv是: " + in.iterator().next());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
