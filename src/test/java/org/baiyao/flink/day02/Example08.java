package org.baiyao.flink.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example08 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                })
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer in, Integer acc) throws Exception {
                        return in + acc;
                    }
                })
                .setParallelism(4)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
