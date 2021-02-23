package com.auspicious.snow.clink.stream.demo;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.util.Collector;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/2/22 17:03
 */
public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setParallelism(1);

        //将list数据转换为datastream
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(new ArrayList<String>() {{
            add("hello,world");
            add("hello,chaixiaoxue");
        }});
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(value -> value.f0).sum(1);
        sum.print();
        env.execute("world count deemo");
    }

}
