package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i < 9; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rescale()
                .print("rescale")
                .setParallelism(4);

        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i < 9; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .rebalance()
                .print("rebalance")
                .setParallelism(4);

        env.execute();
    }
}
