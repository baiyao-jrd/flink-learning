package org.baiyao.flink.day02;


import com.baiyao.flink.util.ClickEvent;
import com.baiyao.flink.util.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 使用filter过滤掉Bob和Alice的相关信息
*
* */
public class Example05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .filter(r -> r.username.equals("Mary"))
                .print("使用匿名函数过滤");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("使用外部类方式过滤");

        env
                .addSource(new ClickSource())
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent in) throws Exception {
                        return in.username.equals("Mary");
                    }
                })
                .print("使用匿名类方式过滤");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<ClickEvent> out) throws Exception {
                        if (in.username.equals("Mary")) {
                            out.collect(
                                in
                            );
                        }
                    }
                })
                .print("使用flatmap实现filter过滤功能");

        env.execute();

    }

    public static class MyFilter implements FilterFunction<ClickEvent> {

        @Override
        public boolean filter(ClickEvent in) throws Exception {
            return in.username.equals("Mary");
        }
    }
}
