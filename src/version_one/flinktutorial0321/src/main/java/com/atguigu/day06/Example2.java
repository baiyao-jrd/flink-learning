package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        // ctx.emitWatermark(new Watermark(Long.MIN_VALUE))
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 3000L);
                        ctx.emitWatermark(new Watermark(10000L));
                        ctx.collectWithTimestamp("a", 13000L);
                        // ctx.emitWatermark(new Watermark(Long.MAX_VALUE))
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("key：" + key + "，窗口：" + new Timestamp(context.window().getStart()) + "~" +
                                "" + new Timestamp(context.window().getEnd()) + "，里面有 " + elements.spliterator().getExactSizeIfKnown() + " 条数据。");
                    }
                })
                .print();

        env.execute();
    }
}
