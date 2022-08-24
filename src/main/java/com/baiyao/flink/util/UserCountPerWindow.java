package com.baiyao.flink.util;

import java.sql.Timestamp;

public class UserCountPerWindow {
    public String username;
    public Long count;
    public Long startTime;
    public Long endTime;

    public UserCountPerWindow() {
    }

    public UserCountPerWindow(String username, Long count, Long startTime, Long endTime) {
        this.username = username;
        this.count = count;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "UserCountPerWindow{" +
                "username='" + username + '\'' +
                ", count=" + count +
                ", startTime=" +new Timestamp(startTime) +
                ", endTime=" + new Timestamp(endTime) +
                '}';
    }
}
