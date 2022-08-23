package com.baiyao.flink.util;

public class UserCountPerWindow {
    public String username;
    public Long count;
    public Long startTime;
    public Long endTime;

    public UserCountPerWindow() {
    }

    public UserCountPerWindow(String username, long count, long startTime, long endTime) {
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
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
