package com.atguigu.day07;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import com.atguigu.util.UserViewCountPerWindow;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ClickEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<ClickEvent>() {
                            @Override
                            public long extractTimestamp(ClickEvent element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.username)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                // 窗口的第一条数据的时间戳之后的每个整数秒都触发一次窗口计算
                .trigger(new Trigger<ClickEvent, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(ClickEvent element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 标志位状态变量，标志是否是窗口的第一条数据
                        // 窗口状态变量，每个窗口维护自己的状态变量
                        ValueState<Boolean> flag = ctx.getPartitionedState(
                                new ValueStateDescriptor<Boolean>(
                                        "flag",
                                        Types.BOOLEAN
                                )
                        );

                        if (flag.value() == null) {
                            // 第一条数据到达
                            // 计算第一条数据时间戳接下来的整数秒
                            // 1234ms => 2000ms
                            long nextSecond = element.ts + 1000L - element.ts % 1000L;
                            // 注册定时器onEventTime
                            ctx.registerEventTimeTimer(nextSecond);
                            flag.update(true);
                        }

                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        if (time < window.getEnd()) {
                            if (time + 1000L < window.getEnd()) {
                                // 注册的还是onEventTime
                                ctx.registerEventTimeTimer(time + 1000L);
                            }
                            return TriggerResult.FIRE;
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        // 单例模式
                        ValueState<Boolean> flag = ctx.getPartitionedState(
                                new ValueStateDescriptor<Boolean>(
                                        "flag",
                                        Types.BOOLEAN
                                )
                        );
                        flag.clear();
                    }
                })
                .aggregate(
                        new AggregateFunction<ClickEvent, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(ClickEvent value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
                                out.collect(new UserViewCountPerWindow(
                                        s,
                                        elements.iterator().next(),
                                        context.window().getStart(),
                                        context.window().getEnd()
                                ));
                            }
                        }
                )
                .print();

        env.execute();
    }
}
