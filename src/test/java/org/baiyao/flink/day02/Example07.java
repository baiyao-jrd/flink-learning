package org.baiyao.flink.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3)
                .map(r -> Tuple2.of(r, r))
                //类型注解
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .print("类型擦除");

        env.execute();
    }
}
