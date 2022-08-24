package com.baiyao.flink.util;

import java.sql.Timestamp;

public class ProductViewCountPerWindow {
    public String username;
    public Long count;
    public Long windowStartTime;
    public Long windowEndTime;

    public ProductViewCountPerWindow() {
    }

    public ProductViewCountPerWindow(String username, Long count, Long windowStartTime, Long windowEndTime) {
        this.username = username;
        this.count = count;
        this.windowStartTime = windowStartTime;
        this.windowEndTime = windowEndTime;
    }

    @Override
    public String toString() {
        return "ProductViewCountPerWindow{" +
                "username='" + username + '\'' +
                ", count=" + count +
                ", windowStartTime=" + new Timestamp(windowStartTime) +
                ", windowEndTime=" + new Timestamp(windowEndTime) +
                '}';
    }
}
