package com.baiyao.flink.util;

public class Event {
    public String key;
    public String value;
    public Long ts;

    public Event() {
    }

    public Event(String key, String value, Long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "键：" + key + "，值：" + value + "，时间戳：" + ts;
    }
}
