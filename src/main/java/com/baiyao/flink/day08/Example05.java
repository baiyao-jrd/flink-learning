package com.baiyao.flink.day08;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Example05 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Long>() {
                    private Boolean running = true;
                    private Long count = 0L;

                    @Override
                    public void run(SourceContext<Long> ctx) throws Exception {
                        while (running) {
                            ctx.collect(count);
                            count++;

                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .addSink(new TransactionalFileSink());

        env.execute();
    }

    public static class TransactionalFileSink extends TwoPhaseCommitSinkFunction<Long, String, Void> {
        public TransactionalFileSink() {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        @Override
        protected void invoke(String transaction, Long value, Context context) throws Exception {

        }

        @Override
        protected String beginTransaction() throws Exception {
            return null;
        }

        @Override
        protected void preCommit(String transaction) throws Exception {

        }

        @Override
        protected void commit(String transaction) {

        }

        @Override
        protected void abort(String transaction) {

        }
    }
}
