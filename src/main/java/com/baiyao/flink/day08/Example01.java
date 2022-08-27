//package com.baiyao.flink.day08;
//
//import com.baiyao.flink.util.Event;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//public class Example01 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        DataStreamSource<Event> leftStream = env
//                .addSource(new SourceFunction<Event>() {
//                    @Override
//                    public void run(SourceContext<Event> ctx) throws Exception {
//                        ctx.collectWithTimestamp(new Event(
//                                "key-01",
//                                "left",
//                                10 * 1000L
//                        ), 10 * 1000L);
//                    }
//
//                    @Override
//                    public void cancel() {
//
//                    }
//                });
//
//        env
//                .addSource(new SourceFunction<Event>() {
//                    @Override
//                    public void run(SourceContext<Event> ctx) throws Exception {
//
//                    }
//
//                    @Override
//                    public void cancel() {
//
//                    }
//                })
//
//        env.execute();
//    }
//}
