package com.baiyao.flink.day06;

import com.baiyao.flink.util.ProductViewCountPerWindow;
import com.baiyao.flink.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Example08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("E:\\flinkcode\\flink-learning\\src\\main\\resources\\UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        String[] strings = in.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                strings[0],
                                strings[1],
                                strings[2],
                                strings[3],
                                Long.parseLong(strings[4]) * 1000L
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
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow in) throws Exception {
                        return Tuple2.of(in.windowStartTime, in.windowEndTime);
                    }
                })
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior in, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
            out.collect(
                    new ProductViewCountPerWindow(key,
                            elements.iterator().next(),
                            ctx.window().getStart(),
                            ctx.window().getEnd())
            );
        }
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long, Long>, ProductViewCountPerWindow, String> {
        private int n;
        private ListState<ProductViewCountPerWindow> liststate;

        public TopN(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "list-state",

                            Types.POJO(ProductViewCountPerWindow.class))
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx, Collector<String> out) throws Exception {
            liststate.add(in);

            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow p : liststate.get()) arrayList.add(p);
            liststate.clear();

            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow o1, ProductViewCountPerWindow o2) {
                    return (int) (o2.count - o1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("============" + new Timestamp(ctx.getCurrentKey().f0) + " ~ "
                    + new Timestamp(ctx.getCurrentKey().f1) + "====================\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow tmp = arrayList.get(i);
                result.append("第" + (i + 1) + "名的商品ID： " + tmp.username + ",浏览次数： " + tmp.count + "\n");
            }
            result.append("===============================================\n");
            out.collect(result.toString());
        }
    }
}
