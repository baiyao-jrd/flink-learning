package org.baiyao.flink.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 自定义 - 指定某key进入某指定的并行子任务中
* */
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .partitionCustom(
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                if (key == 0 || key == 1) {
                                    return 0;
                                } else {
                                    return 1;
                                }
                            }
                        },
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer in) throws Exception {
                                return in % 3;
                            }
                        }
                )
                .print("自定义物理分区")
                .setParallelism(4);
        
        env.execute();
    }
}
