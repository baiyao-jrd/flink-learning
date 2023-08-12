package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        streamTableEnvironment
                .executeSql("CREATE TABLE clicks (`user` STRING, `url` STRING) " +
                        "WITH (" +
                        "'connector' = 'filesystem'," +
                        "'path' = '/home/zuoyuan/flinktutorial0321/src/main/resources/file.txt'," +
                        "'format' = 'csv')");

        streamTableEnvironment
                .executeSql("CREATE TABLE ResultTable (`user` STRING, `cnt` BIGINT) " +
                        "WITH ('connector' = 'print')");

        streamTableEnvironment
                .executeSql("INSERT INTO ResultTable " +
                        "SELECT `user`, COUNT(`user`) as cnt FROM clicks GROUP BY `user`");
    }
}
