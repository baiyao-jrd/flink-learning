package com.atguigu.day09;

import com.atguigu.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("/home/zuoyuan/flinktutorial0321/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        String[] array = in.split(",");
                        return new UserBehavior(
                                array[0], array[1], array[2], array[3],
                                Long.parseLong(array[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.ts;
                                    }
                                })
                );

        // 获取表执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // 将数据流转换成动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        $("userId"),
                        $("productId"),
                        $("categotyId").as("categoryId"),
                        $("type"),
                        $("ts").rowtime() // `.rowtime()`表示这一列是事件时间
                );

        // 将动态表注册成临时视图
        streamTableEnvironment.createTemporaryView("userbehavior", table);

        // 查询
        // apache calcite
        // COUNT关键字是全窗口聚合
        Table result = streamTableEnvironment
                .sqlQuery(
                        "SELECT productId, COUNT(productId)," +
                                "HOP_START(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS) as windowStartTime," +
                                "HOP_END(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS) as windowEndTime " +
                                "FROM userbehavior GROUP BY productId, HOP(ts, INTERVAL '5' MINUTES, INTERVAL '1' HOURS)"
                );

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
