package com.atguigu.day09;

import com.atguigu.util.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

// 实现订单超时检测
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Event, String> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("order-1", "create-order", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("order-2", "create-order", 2000L), 2000L);
                        ctx.collectWithTimestamp(new Event("order-1", "pay-order", 3000L), 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r.key);

        stream
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<Event> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<Event>(
                                        "state",
                                        Types.POJO(Event.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(Event in, Context ctx, Collector<String> out) throws Exception {
                        if (in.value.equals("create-order")) {
                            state.update(in);
                            ctx.timerService().registerEventTimeTimer(in.ts + 5000L);
                        } else if (in.value.equals("pay-order")) {
                            out.collect(in.key + "在" + in.ts + "完成支付");
                            state.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        if (state.value() != null) {
                            out.collect(state.value().key + "超时未支付");
                        }
                    }
                })
                .print();

        env.execute();
    }
}
