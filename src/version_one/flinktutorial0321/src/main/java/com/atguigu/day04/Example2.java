package com.atguigu.day04;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new UserUrlCount())
                .print();

        env.execute();
    }

    public static class UserUrlCount extends KeyedProcessFunction<String, ClickEvent, String> {
        // key: URL
        // value: URL的访问次数
        private MapState<String, Integer> urlCount;
        @Override
        public void open(Configuration parameters) throws Exception {
            // {
            //    username1: {
            //        url1: count1,
            //        url2: count2,
            //        ....
            //    },
            //    username2: {
            //        url1: count1,
            //        url2: count2,
            //        ...
            //    }
            // }
            urlCount = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Integer>(
                            "url-count",
                            Types.STRING, // key的泛型
                            Types.INT     // value的泛型
                    )
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
            // urlCount被约束到了输入数据in的key所对应的HashMap
            if (urlCount.get(in.url) == null) {
                urlCount.put(in.url, 1);
            } else {
                urlCount.put(in.url, urlCount.get(in.url) + 1);
            }

            String result = "";
            result = result + ctx.getCurrentKey() + " {\n";
            for (String url : urlCount.keys()) {
                result = result + "  '" + url + "' => " + urlCount.get(url) + ",\n";
            }
            result = result + "}\n";

            out.collect(result);
        }
    }
}
