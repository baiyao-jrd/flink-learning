package com.atguigu.util;

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
