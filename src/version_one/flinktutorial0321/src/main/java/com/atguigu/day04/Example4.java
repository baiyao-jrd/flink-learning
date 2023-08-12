package com.atguigu.day04;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import com.atguigu.util.UserViewCountPerWindow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 计算每个用户在每5秒钟的访问次数
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                // 5秒钟的滚动处理时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<ClickEvent, UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<ClickEvent> elements, Collector<UserViewCountPerWindow> out) throws Exception {
            // Iterable<ClickEvent> elements包含了属于窗口的所有元素
            // elements.spliterator().getExactSizeIfKnown()获取迭代器中的元素数量
            out.collect(new UserViewCountPerWindow(
                    key,
                    elements.spliterator().getExactSizeIfKnown(),
                    context.window().getStart(),
                    context.window().getEnd()
            ));
        }
    }
}
