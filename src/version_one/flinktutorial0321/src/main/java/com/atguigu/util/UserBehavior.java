package com.atguigu.util;

import java.sql.Timestamp;

public class UserBehavior {
    public String userId;
    public String productId;
    public String categotyId;
    public String type;
    public Long ts;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String productId, String categotyId, String type, Long ts) {
        this.userId = userId;
        this.productId = productId;
        this.categotyId = categotyId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", categotyId='" + categotyId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
