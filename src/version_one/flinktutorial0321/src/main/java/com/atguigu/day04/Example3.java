package com.atguigu.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new TempAlert())
                .print();

        env.execute();
    }

    // 定时器负责报警
    // 当温度出现上升，注册一个1秒钟之后的定时器
    // 当温度出现下降，如果存在报警定时器，则删除定时器
    public static class TempAlert extends KeyedProcessFunction<String, SensorReading, String> {
        // 保存上一次温度值
        private ValueState<Double> lastTemp;
        // 保存定时器的时间戳
        private ValueState<Long> timerTs;
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>(
                            "last-temp",
                            Types.DOUBLE
                    )
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "timer-ts",
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            // 将上一次温度存储到一个临时变量
            Double prevTemp = lastTemp.value();
            // 将当前温度存储到lastTemp
            lastTemp.update(in.temperature);

            if (prevTemp != null) {
                // 第一种情况
                // 温度上升 && 不存在报警定时器
                if (in.temperature > prevTemp && timerTs.value() == null) {
                    long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                    // 注册报警定时器
                    ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                    // 将报警定时器的时间戳保存到timerTs状态变量
                    timerTs.update(oneSecondLater);
                }
                // 第二种情况
                // 温度下降 && 存在报警定时器
                else if (in.temperature < prevTemp && timerTs.value() != null) {
                    // 删除报警定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                    // 将报警定时器时间戳从timerTs中清空
                    timerTs.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续1s温度上升。");
            // 由于执行onTimer，flink会将定时器删除
            // 所以保存定时器时间戳的状态变量也需要清空
            timerTs.clear();
        }
    }

    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 1; i < 4; i++) {
                    ctx.collect(new SensorReading(
                            "sensor_" + i,
                            random.nextGaussian()
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static class SensorReading {
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }
    }
}
