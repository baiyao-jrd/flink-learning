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

// com.atguigu.day04.Example4的底层实现
public class Example1 {
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
        // value: 属于窗口的元素组成的列表
        private MapState<Tuple2<Long, Long>, List<ClickEvent>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, List<ClickEvent>>(
                            "windowinfo-eventlist",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LIST(Types.POJO(ClickEvent.class))
                    )
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // currTs = 2000ms
            // currTs = 3000ms
            long currTs = ctx.timerService().currentProcessingTime();
            // 计算窗口开始时间
            // 0ms
            // 0ms
            long windowStartTime = currTs - currTs % windowSize;
            // 计算窗口结束时间
            // 5000ms
            // 5000ms
            long windowEndTime = windowStartTime + windowSize;
            // 窗口信息元组
            // (0,5000)
            // (0,5000)
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            // 将输入数据分发到所属窗口
            if (!mapState.contains(windowInfo)) {
                // 输入数据是窗口的第一个元素
                ArrayList<ClickEvent> events = new ArrayList<>();
                // [(Mary,./home,2000ms)]
                events.add(in);
                mapState.put(windowInfo, events);
            } else {
                // 输入数据所属窗口已经存在，那么直接将输入数据添加到窗口中
                // [(Mary,./home,2000ms), (Mary,./cart,3000ms)]
                mapState.get(windowInfo).add(in);
            }

            // 在 窗口结束时间-1毫秒 注册一个定时器
            // 4999ms
            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 根据timestamp参数计算出窗口结束时间
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            long count = mapState.get(windowInfo).size();
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
