package com.atguigu.day08;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new CounterSource())
                .print();

        env.execute();
    }

    public static class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
        private Long offset = 0L;
        private boolean running = true;
        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();

            while (running) {
                synchronized (lock) {
                    ctx.collect(offset);
                    offset += 1L;
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        // checkpointedfunction

        // 声明算子状态
        private ListState<Long> state;

        // 程序启动时触发调用
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>(
                            "state",
                            Types.LONG
                    )
            );

            // 迭代器中只有一个元素
            for (Long l : state.get()) {
                offset = l;
            }
        }

        // 检查点分界线进入到source算子时触发调用
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 保证ListState中只有一个元素
            state.clear();
            state.add(offset);
        }
    }
}
