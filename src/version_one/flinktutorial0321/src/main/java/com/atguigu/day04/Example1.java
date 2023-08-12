package com.atguigu.day04;

import com.atguigu.util.IntSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntSource())
                .keyBy(new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 2;
                    }
                })
                .process(new SortHistoryData())
                .print();

        env.execute();
    }

    public static class SortHistoryData extends KeyedProcessFunction<Integer, Integer, String> {
        private ListState<Integer> historyData;

        @Override
        public void open(Configuration parameters) throws Exception {
            // {
            //    0: ArrayList[Integer, ...]
            //    1: ArrayList[Integer, ...]
            // }
            historyData = getRuntimeContext().getListState(
                    new ListStateDescriptor<Integer>(
                            "history",
                            Types.INT
                    )
            );
        }

        @Override
        public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
            // 将in添加到historyData中in的key对应的ArrayList中
            historyData.add(in);

            StringBuilder result = new StringBuilder();
            if (ctx.getCurrentKey() == 0) {
                result.append("偶数历史数据排序结果：");
            } else {
                result.append("奇数历史数据排序结果：");
            }

            // 排序操作
            // 将列表状态变量中的数据都取出来然后放进integers数组中
            ArrayList<Integer> integers = new ArrayList<>();
            for (Integer i : historyData.get()) integers.add(i);
            integers.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer i1, Integer i2) {
                    return i1 - i2;
                }
            });

            for (Integer i : integers) {
                result.append(i + " => ");
            }

            out.collect(result.toString());
        }
    }
}
