package org.baiyao.flink.day02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(1000));
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .setParallelism(1)
                .map(r -> Tuple5.of(r, r, r, 1, r))
                .returns(Types.TUPLE(Types.INT,Types.INT,Types.INT,Types.INT,Types.INT))
                .keyBy(r -> "int")
                .reduce(new ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> acc, Tuple5<Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        return Tuple5.of(
                                Math.max(in.f0, acc.f0),
                                Math.min(in.f1, acc.f1),
                                in.f2 + acc.f2,
                                acc.f3 + 1,
                                (in.f2 + acc.f2) / (acc.f3 + 1)
                        );
                    }
                })
                .print("main");

        env.execute();
    }
}
