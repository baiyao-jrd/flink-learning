package com.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .keyBy(r -> "Socket")
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        long currentTs = ctx.timerService().currentProcessingTime();
                        long thirtySecondsLater = currentTs + 3 * 1000L;
                        long sixtySecondsLater = currentTs + 6 * 1000L;

                        ctx.timerService().registerProcessingTimeTimer(thirtySecondsLater);
                        ctx.timerService().registerProcessingTimeTimer(sixtySecondsLater);

                        out.collect("输入数据: " + in + ", key是： " + ctx.getCurrentKey() + ", 到达时间是： " +
                                "" + new Timestamp(currentTs) + "注册了定时器： " +
                                "" + new Timestamp(thirtySecondsLater) + ", " +
                                "" + new Timestamp(sixtySecondsLater)

                        );
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("key是： " + ctx.getCurrentKey() + "，定时器" +
                                "" + new Timestamp(timestamp) + "定时器真正执行的机器时间是： " +
                                "" + new Timestamp(ctx.timerService().currentProcessingTime())
                        );
                    }
                })
                .print();

        env.execute();
    }
}
