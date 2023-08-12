package com.atguigu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3)
                .map(new RichMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("并行子任务的索引：" + getRuntimeContext().getIndexOfThisSubtask() + "，" +
                                "生命周期开始");
                    }

                    @Override
                    public String map(Integer in) throws Exception {
                        return "并行子任务索引：" + getRuntimeContext().getIndexOfThisSubtask() + "，" +
                                "处理数据：" + in;
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("并行子任务的索引：" + getRuntimeContext().getIndexOfThisSubtask() + "，" +
                                "生命周期结束");
                    }
                })
                .setParallelism(2)
                .print();

        env.execute();
    }
}
