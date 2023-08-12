package com.atguigu.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 自定义分区
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .partitionCustom(
                        // 匿名类，用来设定将哪一个key的数据发送到哪一个并行子任务
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                // 如果输入数据的key是0，将数据路由到print的索引为0的并行子任务
                                if (key == 0) {
                                    return 0;
                                } else if (key == 1) {
                                    return 1;
                                } else {
                                    return 2;
                                }
                            }
                        },
                        // 指定输入数据的key
                        new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer in) throws Exception {
                                return in % 3;
                            }
                        }
                )
                .print()
                .setParallelism(4);

        env.execute();
    }
}
