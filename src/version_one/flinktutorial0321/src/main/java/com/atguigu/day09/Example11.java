package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        streamTableEnvironment
                .executeSql("CREATE TABLE clicks (" +
                        "`user` STRING," +
                        "`url` STRING," +
                        "`ts` TIMESTAMP(3)," +
                        "WATERMARK FOR `ts` AS `ts` - INTERVAL '3' SECONDS) " +
                        "WITH (" +
                        "'connector' = 'filesystem'," +
                        "'path' = '/home/zuoyuan/flinktutorial0321/src/main/resources/file1.csv'," +
                        "'format' = 'csv')");

        streamTableEnvironment
                .executeSql("CREATE TABLE ResultTable (" +
                        "`user` STRING," +
                        "`cnt` BIGINT," +
                        "windowStartTime TIMESTAMP(3)," +
                        "windowEndTime TIMESTAMP(3)) " +
                        "WITH ('connector' = 'print')");

        streamTableEnvironment
                .executeSql("INSERT INTO ResultTable " +
                        "SELECT `user`, COUNT(`user`) as cnt, " +
                        "TUMBLE_START(`ts`, INTERVAL '5' SECONDS) as windowStartTime, " +
                        "TUMBLE_END(`ts`, INTERVAL '5' SECONDS) as windowEndTime " +
                        "FROM clicks GROUP BY `user`, TUMBLE(`ts`, INTERVAL '5' SECONDS)");
    }
}
