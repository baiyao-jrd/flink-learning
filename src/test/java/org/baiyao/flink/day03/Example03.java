package org.baiyao.flink.day03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example03 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4)
                .setParallelism(1)
                .flatMap(new RichFlatMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("并行子任务" + getRuntimeContext().getIndexOfThisSubtask() + "开始");
                    }

                    @Override
                    public void flatMap(Integer in, Collector<String> out) throws Exception {
                        out.collect("并行子任务索引： " + getRuntimeContext().getIndexOfThisSubtask() + ", " + " 处理数据为：" + in);
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("并行子任务" + getRuntimeContext().getIndexOfThisSubtask() + "结束");
                    }
                })
                .setParallelism(2)
                .print("富函数 - 生命周期")
                .setParallelism(2);


        env.execute();
    }
}
