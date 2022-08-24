package com.baiyao.flink.day05;

import com.baiyao.flink.util.ClickEvent;
import com.baiyao.flink.util.ClickSource;
import com.baiyao.flink.util.UserCountPerWindow;
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

public class Example01 {
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

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, ClickEvent, UserCountPerWindow> {
        private long windowSize;
        private MapState<Tuple2<Long, Long>, List<ClickEvent>> mapState;


        public MyTumblingProcessingTimeWindow(long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, List<ClickEvent>>(
                            "windowInfo-event",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LIST(Types.POJO(ClickEvent.class))
                    )
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserCountPerWindow> out) throws Exception {
            long currts = ctx.timerService().currentProcessingTime();
            long windowStart = currts - currts % windowSize;
            long windowEnd = windowStart + windowSize;

            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStart, windowEnd);

            if (!mapState.contains(windowInfo)) {
                ArrayList<ClickEvent> events = new ArrayList<>();
                events.add(in);
                mapState.put(windowInfo, events);
            } else {
                mapState.get(windowInfo).add(in);
            }

            ctx.timerService().registerProcessingTimeTimer(windowEnd - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserCountPerWindow> out) throws Exception {
            long windowStart =  timestamp + 1L;
            long windowEnd = windowStart + windowSize;

            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStart, windowEnd);

            long count = mapState.get(windowInfo).size();

            String username = ctx.getCurrentKey();

            out.collect(
                    new UserCountPerWindow(
                            username,
                            count,
                            windowStart,
                            windowEnd
                    )
            );

            mapState.remove(windowInfo);
        }
    }
}

