package org.baiyao.flink.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Example09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(r -> "number")
                .process(new Statistics())
                .print();

        env.execute();
    }

    public static class Statistics extends KeyedProcessFunction<String, Integer, IntStatistics> {
        private ValueState<IntStatistics> accumulator;

        @Override
        public void open(Configuration parameters) throws Exception {
            accumulator = getRuntimeContext().getState(
                    new ValueStateDescriptor<IntStatistics>(
                            "accumulator",
                            Types.POJO(IntStatistics.class))
            );
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<IntStatistics> out) throws Exception {
            if (accumulator.value() == null) {
                accumulator.update(
                        new IntStatistics(
                                in,
                                in,
                                in,
                                1,
                                in
                        )
                );
            } else {
                IntStatistics oldAcc = accumulator.value();
                accumulator.update(
                        new IntStatistics(
                                Math.max(in,oldAcc.max),
                                Math.min(in,oldAcc.min),
                                in + oldAcc.sum,
                                1 + oldAcc.count,
                                (in + oldAcc.sum) / (1 + oldAcc.count)
                        )
                );
            }

            out.collect(accumulator.value());
        }
    }

    public static class IntStatistics {
        public Integer max;
        public Integer min;
        public Integer sum;
        public Integer count;
        public Integer avg;

        public IntStatistics() {
        }

        public IntStatistics(Integer max, Integer min, Integer sum, Integer count, Integer avg) {
            this.max = max;
            this.min = min;
            this.sum = sum;
            this.count = count;
            this.avg = avg;
        }

        @Override
        public String toString() {
            return "最大值：" + max + ", " +
                    "" + "最小值：" + min + ", " +
                    "" + "总和：" + sum + ", " +
                    "" + "总数：" + count + ", " +
                    "" + "平均值：" + avg;
        }
    }

    public static class IntSource implements SourceFunction<Integer> {
        Random random = new Random();
        private Boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt(1000));

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
