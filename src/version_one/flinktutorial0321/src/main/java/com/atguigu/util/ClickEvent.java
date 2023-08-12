package com.atguigu.util;

import java.sql.Timestamp;

// case class ClickEvent(username : String, url : String, ts : Long)
public class ClickEvent {
    public String username;
    public String url;
    public Long ts;

    public ClickEvent() {
    }

    public ClickEvent(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "username='" + username + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
