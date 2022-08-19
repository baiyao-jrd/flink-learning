package com.baiyao.flink.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/*
* 整型数据源： 数据流不断地输出随机整数
* */
public class IntSource implements SourceFunction<Integer> {
    private boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running) {
            //1. 生成1000以内的随机整数
            ctx.collect(random.nextInt(1000));
            //2. 每间隔1秒发一个
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
