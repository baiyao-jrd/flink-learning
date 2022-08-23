//package com.baiyao.flink.day04;
//
//import com.baiyao.flink.util.ClickEvent;
//import com.baiyao.flink.util.ClickSource;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//public class Example02 {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        env
//                .addSource(new ClickSource())
//                .keyBy(r -> r.username)
//                .process(new UserUrlCount())
//                .print();
//
//        env.execute();
//    }
//
//    public static class UserUrlCount extends KeyedProcessFunction<String, ClickEvent, String> {
//        private MapState<String, Integer> urlCount;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            urlCount = getRuntimeContext().getMapState(
////                    new MapStateDescriptor<String, Integer>("url-count", Types.STRING)
//            );
//        }
//
//        @Override
//        public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
//
//        }
//    }
//}
