package com.baiyao.flink.day02;

import com.baiyao.flink.util.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 自定义数据源，不断生成自定义的随机数据
* */
public class Example3 {
    public static void main(String[] args) throws Exception{
        //1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 为了方便，直接设置全局并行度
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .print();

        env.execute();
    }
}
