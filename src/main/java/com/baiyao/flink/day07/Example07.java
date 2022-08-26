package com.baiyao.flink.day07;

import com.baiyao.flink.util.ClickEvent;
import com.baiyao.flink.util.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Example07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> stream1 = env.addSource(new ClickSource());

        DataStreamSource<String> stream2 = env.socketTextStream("hadoop102", 9999);

        stream1.connect(stream2).flatMap(new MyConnect()).print();

        env.execute();
    }

    public static class MyConnect implements CoFlatMapFunction<ClickEvent,String,ClickEvent> {
        private String user_name = "";

        @Override
        public void flatMap1(ClickEvent in, Collector<ClickEvent> out) throws Exception {
            if (in.username.equals(user_name)) {
                out.collect(in);
            }
        }

        @Override
        public void flatMap2(String in, Collector<ClickEvent> out) throws Exception {
            user_name = in;
        }


    }
}
