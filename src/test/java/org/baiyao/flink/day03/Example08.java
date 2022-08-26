package org.baiyao.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/*
 * keyedprocessfunction + 定时器
 * */
public class Example08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("hadoop102", 9999)
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        Long curtts = ctx.timerService().currentProcessingTime();

                        Long thirtySeconds = curtts + 3 * 1000L;
                        Long sixtySeconds = curtts + 6 * 1000L;

                        ctx.timerService().registerProcessingTimeTimer(thirtySeconds);
                        ctx.timerService().registerProcessingTimeTimer(sixtySeconds);

                        out.collect("key为：" + ctx.getCurrentKey() + "的数据" + in + "，在" + new Timestamp(curtts) + "时刻被处理，" +
                                "" + "其注册的第一个定时器为： " + new Timestamp(thirtySeconds) +
                                "" + "，其注册的第二个定时器为： " + new Timestamp(sixtySeconds));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("数据" + ctx.getCurrentKey() + "在" + new Timestamp(ctx.timerService().currentProcessingTime())  +
                                "" + "时间被处理，" + "定时器时间戳为：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
