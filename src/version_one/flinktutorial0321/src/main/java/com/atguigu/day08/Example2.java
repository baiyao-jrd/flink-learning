package com.atguigu.day08;

import com.atguigu.util.Event;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

//基于窗口的join
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "left", 2 * 1000L), 2 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "left", 6 * 1000L), 6 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "left", 13 * 1000L), 13 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStreamSource<Event> rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("key-1", "right", 2 * 1000L), 2 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-1", "right", 8 * 1000L), 8 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "right", 12 * 1000L), 12 * 1000L);
                        ctx.collectWithTimestamp(new Event("key-2", "right", 22 * 1000L), 22 * 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        leftStream
                .join(rightStream)
                .where(r -> r.key)
                .equalTo(r -> r.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) throws Exception {
                        return first + " => " + second;
                    }
                })
                .print();

        env.execute();
    }
}
