package com.baiyao.flink.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/*
* 实现自定义数据源
*
* 实现SourceFunction接口
* */
public class ClickSource implements SourceFunction<ClickEvent> {
    /*
    * 自定义数据源，自己先造一些数据
    * */
    private boolean running = true;
    //3. 实例化一个随机数发生器
    private Random random = new Random();
    //4. 自定义数组
    private String[] userArray = {"Mary","Bob","Alice"};
    private String[] urlArray = {"./home","./cart","./buy"};


    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        //1. 自定义一个初始值为true的标志位running，可以不断地发送数据
        while (running) {
            //5. 收集将要发送的数据 - clickEvent实例;
            ctx.collect(new ClickEvent(
                    //6. 随即取出一个用户名
                    userArray[random.nextInt(userArray.length)],
                    urlArray[random.nextInt(urlArray.length)],
                    //7. 获取当前系统时间的毫秒时间戳
                    Calendar.getInstance().getTimeInMillis()
            ));
            //8. 间隔1秒发送数据
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        //2. 取消任务的时候，只需要把标志位置为false就行了
        running = false;
    }

}


