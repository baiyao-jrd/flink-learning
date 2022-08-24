package com.baiyao.flink.day06;

import akka.protobuf.ByteString;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 2000L);
                        ctx.emitWatermark(new Watermark(1900L));
                        ctx.collectWithTimestamp("a", 1500L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            ctx.output(
                                    new OutputTag<String>("late-event") {
                                    }
                                    , "数据" + in + "," + ctx.timestamp() + "没迟到");
                        } else {
                            out.collect("数据" + in + "," + ctx.timestamp() + "没迟到");
                        }
                    }
                });

        result.print("main-主流");
        result.getSideOutput(new OutputTag<String>("late-event")).print("测流输出");

        env.execute();
    }
}
