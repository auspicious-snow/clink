package com.auspicious.snow.clink.stream.demo;

import java.util.ArrayList;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
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
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);可以设置模式 流 批 自动
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setParallelism(1);

        //将list数据转换为datastream
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(new ArrayList<String>() {{
            add("hello,world");
            add("hello,chaixiaoxue");
        }});
        //也可以得到一个source
        DataStreamSource<String> stringDataStreamSource2 = env.fromElements("spark hadoop flink","flink test","hadoop");
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            for (String s : value.split(",")) {
                out.collect(new Tuple2<>(s, 1));
            }//dataset中分组是group by  dastastream 中是keyby
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(value -> value.f0).sum(1);
        sum.print();
        env.execute("world count demo");
    }

}
