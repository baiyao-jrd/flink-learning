package com.baiyao.flink.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 * 【自定义分区】
 *
 * */
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                if (key == 0) {
                                    return 0;
                                } else if (key == 1) {
                                    return 1;
                                } else {
                                    return 2;
                                }
                            }
                        }, new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer in) throws Exception {
                                return in % 3;
                            }
                        })
                .print()
                .setParallelism(4);

        env.execute();
    }
}
