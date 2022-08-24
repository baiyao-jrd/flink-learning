package org.baiyao.flink.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<ClickEvent> {
    Random random = new Random();
    private Boolean running = true;
    private String[] usernameArray = {"Mary","Bob","Alice"};
    private String[] urlArray = {"./home","./cart","./buy"};

    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while (running) {
            ctx.collect(
                    new ClickEvent(
                            usernameArray[random.nextInt(usernameArray.length)],
                            urlArray[random.nextInt(urlArray.length)],
                            Calendar.getInstance().getTimeInMillis()
                    )
            );

            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
