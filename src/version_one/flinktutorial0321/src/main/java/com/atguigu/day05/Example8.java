package com.atguigu.day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    // run函数执行之前，flink会发送一条-MAX的水位线
                    // run函数执行完毕，flink会发送一条+MAX的水位线
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.emitWatermark(new Watermark(-10000L));
                        // 第一个参数：发送的数据
                        // 第二个参数：发送的数据的事件时间
                        ctx.collectWithTimestamp("hello", 1000L);
                        ctx.emitWatermark(new Watermark(10000L));
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 1000L);
                        out.collect("key:" + ctx.getCurrentKey() + "，输入数据：" + in + "，事件时间：" +
                                "" + ctx.timestamp() + ",注册的定时器时间戳：" + (ctx.timestamp() + 1000L) +
                                "，当前process的水位线：" +
                                "" + ctx.timerService().currentWatermark());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key:" + ctx.getCurrentKey() + "，定时器时间戳：" + timestamp + ",当前process的水位线是：" +
                                "" + ctx.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
