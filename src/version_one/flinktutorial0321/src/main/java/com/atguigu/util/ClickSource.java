package com.atguigu.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<ClickEvent> {
    private boolean running = true;
    private Random random = new Random();
    private String[] userArray = {"Mary", "Bob", "Alice"};
    private String[] urlArray = {"./home", "./cart", "./buy"};
    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while (running) {
            ctx.collect(new ClickEvent(
                    userArray[random.nextInt(userArray.length)],
                    urlArray[random.nextInt(urlArray.length)],
                    // 当前系统时间的毫秒时间戳
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
