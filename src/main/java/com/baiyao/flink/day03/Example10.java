package com.baiyao.flink.day03;

import com.baiyao.flink.util.IntSource;
import org.apache.calcite.util.Static;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example10 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "int")
                .process(new myReduce())
                .print();

        env.execute();
    }

    public static class myReduce extends KeyedProcessFunction<String, Integer, Static> {

        @Override
        public void processElement(Integer in, Context context, Collector<Static> collector) throws Exception {

        }
    }
}
