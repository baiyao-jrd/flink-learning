package com.atguigu.day07;

import com.atguigu.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0321/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] array = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                        if (userBehavior.type.equals("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                )
                .keyBy(r -> "userbehavior")
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Tuple2<BloomFilter<String>, Long>, Long>() {
                            @Override
                            public Tuple2<BloomFilter<String>, Long> createAccumulator() {
                                return Tuple2.of(
                                        BloomFilter.create(
                                                // 去重的数据类型
                                                Funnels.stringFunnel(Charsets.UTF_8),
                                                // 预估的去重的数据量
                                                20000,
                                                // 误判率
                                                0.001
                                        ),
                                        0L // 计数器
                                );
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> add(UserBehavior value, Tuple2<BloomFilter<String>, Long> accumulator) {
                                // 如果用户之前一定没来过，计数器加一
                                if (!accumulator.f0.mightContain(value.userId)) {
                                    accumulator.f0.put(value.userId);
                                    accumulator.f1 += 1L;
                                }
                                return accumulator;
                            }

                            @Override
                            public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
                                return accumulator.f1;
                            }

                            @Override
                            public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                                out.collect("窗口" + new Timestamp(context.window().getStart()) + "~" +
                                        "" + new Timestamp(context.window().getEnd()) + "的uv是：" +
                                        "" + elements.iterator().next());
                            }
                        }
                )
                .print();

        env.execute();

    }
}
