package com.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/*
 * [定义并行数据源]
 * */
public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        for (int i = 1; i < 9; i++) {
                            ctx.collect("并行子任务索引： " + getRuntimeContext().getIndexOfThisSubtask() + ", 发送数据： " + i);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(2);

        env.execute();
    }
}
