package org.baiyao.flink.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.baiyao.flink.util.ClickEvent;
import org.baiyao.flink.util.ClickSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Example04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<ClickEvent> {
        private Connection connection;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/userbehavior?useSSL=false"
                    , "root", "000000");
            insertStmt = connection.prepareStatement(
                    "insert into clicks (username,url ) values (?,?)"
            );
            updateStmt = connection.prepareStatement(
                    "update clicks set url = ? where username = ?"
            );

        }

        @Override
        public void invoke(ClickEvent in, Context ctx) throws Exception {
            /*
            * 幂等性写入，先更新，更新失败就插入
            * */
            updateStmt.setString(2, in.username);
            updateStmt.setString(1, in.url);
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(2, in.url);
                insertStmt.setString(1, in.username);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
