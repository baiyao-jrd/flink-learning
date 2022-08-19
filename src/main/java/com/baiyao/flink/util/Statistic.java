package com.baiyao.flink.util;

/*
* 定义pojo类
* 用来保存统计数据
* */
public class Statistic {
    public Integer min;
    public Integer max;
    public Integer sum;
    public Integer count;
    public Integer avg;

    public Statistic() {
    }

    public Statistic(Integer min, Integer max, Integer sum, Integer count, Integer avg) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "Statistic{" +
                "min=" + min +
                ", max=" + max +
                ", sum=" + sum +
                ", count=" + count +
                ", avg=" + avg +
                '}';
    }
}
