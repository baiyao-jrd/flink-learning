package com.baiyao.flink.util;

import java.sql.Timestamp;

public class UserBehavior {
    public String userId;
    public String productId;
    public String categoryId;
    public String type;
    public long ts;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String productId, String categoryId, String type, long ts) {
        this.userId = userId;
        this.productId = productId;
        this.categoryId = categoryId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
