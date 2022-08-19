package com.baiyao.flink.day02;

import com.baiyao.flink.util.ClickEvent;
import com.baiyao.flink.util.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
* 【filter】- 过滤
*
* 针对流里面的每一条数据，输出0个或者1个
*
* 实现功能： 把mary的访问数据给过滤出来
*
* */
public class Example5 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                //1. filter泛型只有一个，因为其输入和输出类型是一样的
                .filter(new FilterFunction<ClickEvent>() {
                    @Override
                    public boolean filter(ClickEvent in) throws Exception {
                        //2. return值为true，这条数据放行，false就过滤掉
                        return in.username.equals("Mary");
                    }
                })
                .print("使用匿名类的方式");

        env
                .addSource(new ClickSource())
                .filter(new MyFilter())
                .print("使用外部类方式");

        env
                .addSource(new ClickSource())
                //3. 输入数据经过λ表达式输出结果为真就放行

                //没有进行类型擦除，为什么？
                //--r.username.equals("Mary")返回值类型为布尔值，为基本数据类型，不会被擦除掉
                .filter(r -> r.username.equals("Mary"))
                .print("使用lambda表达式方式");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, ClickEvent>() {
                    @Override
                    public void flatMap(ClickEvent in, Collector<ClickEvent> out) throws Exception {
                        if (in.username.equals("Mary")) out.collect(in);
                    }
                })
                .print("使用flatmap方式");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<ClickEvent> {

        @Override
        public boolean filter(ClickEvent in) throws Exception {
            return in.username.equals("Mary");
        }
    }
}
