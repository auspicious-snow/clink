package com.auspicious.snow.state.checkpoint.process;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.OptionalLong;
import java.util.stream.LongStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.util.Collector;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/3/24 18:01
 */
public class ProcessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setParallelism(1);
        //将list数据转换为datastream
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(new ArrayList<String>() {{
            add("hello,world");
            add("hello,chaixiaoxue");
        }});
        stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        }).keyBy(value -> value.f0).process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Object>() {

            private ValueState<CountWithTimestamp> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Object> out) throws Exception {
                CountWithTimestamp current = state.value();
                if (current == null) {
                    current = new CountWithTimestamp();
                    current.key = value.f0;
                }

                // update the state's count
                current.count++;

                // set the state's timestamp to the record's assigned event time timestamp
                current.lastModified = ctx.timestamp();

                // write the state back
                state.update(current);

                // schedule the next timer 60 seconds from the current event time
                ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                CountWithTimestamp result = state.value();

                // check if this is an outdated timer or the latest timer
                if (timestamp == result.lastModified + 60000) {
                    // emit the state on timeout
                    out.collect(new Tuple2<String, Long>(result.key, result.count));
                }
            }
        });
        env.execute("world count deemo");
    }

    /*@Test
    public void test03(){

        Instant start = Instant.now();

        //顺序流
        long reduce = LongStream.rangeClosed(0, 100000000L)
            .reduce(0, Long::sum);

        //使用parallel()并行流
        OptionalLong reduce1 = LongStream.rangeClosed(0, 100000000L)
            .parallel()//并行
            .reduce(Long::sum);

        Instant end = Instant.now();
        System.out.println(Duration.between(start,end).toMillis());

    }*/


}
