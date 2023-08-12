package com.atguigu.day08;

import com.atguigu.util.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置每隔10s保存一次检查点
        env.enableCheckpointing(10 * 1000L);
        // 设置保存检查点文件的路径
        env.setStateBackend(new FsStateBackend("file://" + "/home/zuoyuan/flinktutorial0321/src/main/resources/ckpts"));

        env
                .addSource(new ClickSource())
                .print();

        env.execute();
    }
}
