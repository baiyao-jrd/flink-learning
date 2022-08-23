package com.baiyao.flink.util;

import java.sql.Timestamp;

public class ProductViewCountPerWindow {
    public String username;
    public long count;
    public long windowStartTime;
    public long windowEndTime;

    public ProductViewCountPerWindow() {
    }

    public ProductViewCountPerWindow(String username, long count, long windowStartTime, long windowEndTime) {
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
