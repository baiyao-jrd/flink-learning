package com.atguigu.day09;

import com.atguigu.util.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Map;

// 使用flink-cep检测连续三次登录失败
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> ctx) throws Exception {
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 1000L), 1000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 2000L), 2000L);
                        ctx.collectWithTimestamp(new Event("user-2", "success", 3000L), 3000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 4000L), 4000L);
                        ctx.collectWithTimestamp(new Event("user-1", "fail", 5000L), 5000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        // 定义模板
        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("login-fail")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event in) throws Exception {
                        return in.value.equals("fail");
                    }
                })
                // 发生3次
                .times(3)
                // 要求3次事件紧邻发生
                .consecutive();


        // 使用模板在数据流上检测连续三次登录失败
        CEP
                .pattern(stream.keyBy(r -> r.key), pattern)
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        // {
                        //   "login-fail": [Event, Event, Event],
                        // }
                        Event first = map.get("login-fail").get(0);
                        Event second = map.get("login-fail").get(1);
                        Event third = map.get("login-fail").get(2);

                        return first.key + "连续3次登录失败，登录时间戳是：" + first.ts + ";" + second.ts + ";" + third.ts;
                    }
                })
                .print();


        env.execute();
    }
}
