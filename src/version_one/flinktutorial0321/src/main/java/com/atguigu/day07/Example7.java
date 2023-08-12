package com.atguigu.day07;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

// 查询流
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<ClickEvent> clickSource = env.addSource(new ClickSource());

        DataStreamSource<String> queryStream = env.socketTextStream("localhost", 9999);

        clickSource
                .connect(queryStream)
                .flatMap(new Query())
                .print();

        env.execute();
    }

    public static class Query implements CoFlatMapFunction<ClickEvent, String, ClickEvent> {
        private String queryString = "";
        @Override
        public void flatMap1(ClickEvent value, Collector<ClickEvent> out) throws Exception {
            if (value.username.equals(queryString)) out.collect(value);
        }

        @Override
        public void flatMap2(String value, Collector<ClickEvent> out) throws Exception {
            queryString = value;
        }
    }
}
