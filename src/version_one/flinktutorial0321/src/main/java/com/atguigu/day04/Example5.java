package com.atguigu.day04;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import com.atguigu.util.UserViewCountPerWindow;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 计算每个用户在每5秒钟的访问次数
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                // 5秒钟的滚动处理时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    // 输入的泛型是Long类型！！！
    public static class WindowResult extends ProcessWindowFunction<Long, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            // Iterable<Long> elements只有一个元素
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.iterator().next(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }

    public static class CountAgg implements AggregateFunction<ClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ClickEvent value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
