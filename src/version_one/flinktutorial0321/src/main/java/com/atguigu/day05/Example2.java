package com.atguigu.day05;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import com.atguigu.util.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// com.atguigu.day04.Example5的底层实现
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingProcessingTimeWindow(5000L))
                .print();

        env.execute();
    }

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, ClickEvent, UserViewCountPerWindow> {
        private long windowSize; // 窗口大小

        public MyTumblingProcessingTimeWindow(long windowSize) {
            this.windowSize = windowSize;
        }

        // key: (窗口开始时间，窗口结束时间)
        // value: 窗口的累加器
        private MapState<Tuple2<Long, Long>, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, Long>(
                            "windowinfo-eventlist",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            long currTs = ctx.timerService().currentProcessingTime();
            // 计算窗口开始时间
            long windowStartTime = currTs - currTs % windowSize;
            // 计算窗口结束时间
            long windowEndTime = windowStartTime + windowSize;
            // 窗口信息元组
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            // 将输入数据分发到所属窗口
            if (!mapState.contains(windowInfo)) {
                mapState.put(windowInfo, 1L);
            } else {
                mapState.put(windowInfo, mapState.get(windowInfo) + 1L);
            }

            // 在 窗口结束时间-1毫秒 注册一个定时器
            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 根据timestamp参数计算出窗口结束时间
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            long count = mapState.get(windowInfo);
            String username = ctx.getCurrentKey();
            out.collect(new UserViewCountPerWindow(
                    username,
                    count,
                    windowStartTime,
                    windowEndTime
            ));
            // 将窗口销毁
            mapState.remove(windowInfo);
        }
    }
}
