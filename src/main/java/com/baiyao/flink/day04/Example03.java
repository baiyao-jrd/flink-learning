package com.baiyao.flink.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Example03 {
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

    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 0; i < 3; i++) {
                    ctx.collect(new SensorReading(
                            "Sensor_" + i,
                            random.nextGaussian()
                    ));
                }
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

    private static class TempAlert extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Long> timerTs;
        private ValueState<Double> lastTemp;

        @Override
        public void open(Configuration parameters) throws Exception {
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "timer-Ts",
                            Types.LONG
                    )
            );

            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>(
                            "last-temp",
                            Types.DOUBLE
                    )
            );
        }

        @Override
        public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
            Long ts = timerTs.value();
            Double preTemp = lastTemp.value();
            lastTemp.update(in.temperature);

            if (preTemp != null) {
                if (in.temperature > preTemp && ts == null) {
                    Long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                    ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                    timerTs.update(oneSecondLater);
                } else if (in.temperature < preTemp && ts != null) {
                    ctx.timerService().deleteProcessingTimeTimer(ts);
                    timerTs.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续1S温都上升");
            timerTs.clear();
        }
    }
}
