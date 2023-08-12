package com.atguigu.day06;

import com.atguigu.util.ProductViewCountPerWindow;
import com.atguigu.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

// 实时热门商品
// 统计滑动窗口(窗口长度1小时，滑动距离5分钟)里面浏览次数最多的3个商品
public class Example9 {
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
                .keyBy(r -> r.productId)
                .process(new MySlidingEventTimeWindow(60 * 60 * 1000L, 5 * 60 * 1000L))
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

    public static class MySlidingEventTimeWindow extends KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow> {
        private long windowSize;
        private long windowSlide;

        public MySlidingEventTimeWindow(long windowSize, long windowSlide) {
            this.windowSize = windowSize;
            this.windowSlide = windowSlide;
        }

        // key: windowinfo
        // value: acc
        private MapState<Tuple2<Long, Long>, Long> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, Long>(
                            "windowinfo-acc",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(UserBehavior in, Context ctx, Collector<ProductViewCountPerWindow> out) throws Exception {
            // 计算所属的第一个滑动窗口的开始时间
            long startTime = in.ts - in.ts % windowSlide;
            ArrayList<Tuple2<Long, Long>> windowInfoArray = new ArrayList<>();
            for (long start = startTime; start > in.ts - windowSize; start = start - windowSlide) {
                windowInfoArray.add(Tuple2.of(start, start + windowSize));
            }

            for (Tuple2<Long, Long> windowInfo : windowInfoArray) {
                if (!mapState.contains(windowInfo)) {
                    mapState.put(windowInfo, 1L);
                } else {
                    mapState.put(windowInfo, mapState.get(windowInfo) + 1L);
                }

                ctx.timerService().registerEventTimeTimer(windowInfo.f1 - 1L);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProductViewCountPerWindow> out) throws Exception {
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            long count = mapState.get(windowInfo);
            String productId = ctx.getCurrentKey();
            out.collect(new ProductViewCountPerWindow(
                    productId, count, windowStartTime, windowEndTime
            ));
            mapState.remove(windowInfo);
        }
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long, Long>, ProductViewCountPerWindow, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        private ListState<ProductViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "list-state",
                            Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx, Collector<String> out) throws Exception {
            listState.add(in);

            // 保证所有的属于in.windowStartTime ~ in.windowEndTime的ProductViewCountPerWindow都添加到listState中
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ProductViewCountPerWindow> arrayList = new ArrayList<>();
            for (ProductViewCountPerWindow p : listState.get()) arrayList.add(p);
            // 手动gc
            listState.clear();

            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow p1, ProductViewCountPerWindow p2) {
                    return (int)(p2.count - p1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("============" + new Timestamp(ctx.getCurrentKey().f0) + "~" + new Timestamp(ctx.getCurrentKey().f1) + "============\n");
            for (int i = 0; i < n; i++) {
                ProductViewCountPerWindow tmp = arrayList.get(i);
                result.append("第" + (i+1) + "名的商品ID：" + tmp.productId + ",浏览次数：" + tmp.count + "\n");
            }
            result.append("===================================================================\n");
            out.collect(result.toString());
        }
    }
}
