//package com.baiyao.flink.day05;
//
//import com.baiyao.flink.util.ClickEvent;
//import com.baiyao.flink.util.ClickSource;
//import com.baiyao.flink.util.UserCountPerWindow;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//public class Example01 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        env
//                .addSource(new ClickSource())
//                .keyBy(r -> r.username)
//                .process(new MyTumblingProcessingTimeWindow(5000L))
//                .print();
//
//        env.execute();
//    }
//
//    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, ClickEvent, UserCountPerWindow> {
//        private long windowSize;
//
//
//        public MyTumblingProcessingTimeWindow(long windowSize) {
//            this.windowSize = windowSize;
//        }
//
//        @Override
//        public void processElement(ClickEvent in, Context ctx, Collector<UserCountPerWindow> collector) throws Exception {
//            if () {
//
//            } else {
//
//            }
//        }
//    }
//}
//
