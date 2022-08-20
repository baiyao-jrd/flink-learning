package com.baiyao.flink.day03;

import com.baiyao.flink.util.ClickEvent;
import com.baiyao.flink.util.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Example06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .addSink(new MyJDBC());

        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<ClickEvent> {
        private Connection connection;
        private PreparedStatement insertStatement;
        private PreparedStatement updateStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取数据库的连接
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/clicks?useSSL=false",
                    "root",
                    "000000");

            insertStatement = connection.prepareStatement("INSERT INTO clicks (username, url) VALUES (?, ?)");
            updateStatement = connection.prepareStatement("UPDATE clicks SET url = ? WHERE username = ?");
        }

        @Override
        public void close() throws Exception {
            // 关闭数据库的连接
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }

        // 每来一条数据，调用一次
        // 幂等性写入mysql
        @Override
        public void invoke(ClickEvent in, Context context) throws Exception {
            updateStatement.setString(1, in.url);
            updateStatement.setString(2, in.username);
            updateStatement.execute();

            // 如果更新失败
            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1, in.username);
                insertStatement.setString(2, in.url);
                insertStatement.execute();
            }
        }
    }
}
