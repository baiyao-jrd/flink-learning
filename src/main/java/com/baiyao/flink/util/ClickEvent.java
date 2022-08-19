package com.baiyao.flink.util;

import java.sql.Timestamp;

/*
* pojo类: 公有类、公有字段、空构造器
*
* 用来抽象点击流
*
*
* scala中样例类就一行搞定：
*
* case class ClickEvent(username: String, url : String, ts : Long)
* */
public class ClickEvent {
    public String username;
    public String url;
    public Long ts;

    public ClickEvent() {
    }

    //1. 为了实例化一个点击事件，必有需要有一个全字段的构造器

    public ClickEvent(String username, String url, Long ts) {
        this.username = username;
        this.url = url;
        this.ts = ts;
    }

    //2. 用本地时间戳 new Timestamp(ts) : java.sql.Timestamp
    @Override
    public String toString() {
        return "ClickEvent{" +
                "username='" + username + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
