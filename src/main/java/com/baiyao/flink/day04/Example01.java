package com.baiyao.flink.day04;

import com.baiyao.flink.util.IntSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 2;
                    }
                })
                .process(new SortHistoryData())
                .print();

        env.execute();
    }

    public static class SortHistoryData extends KeyedProcessFunction<Integer,Integer,String> {

        @Override
        public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {

        }
    }
}
