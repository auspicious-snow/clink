package com.auspicious.snow.clink.stream.table;

import static org.apache.flink.table.api.Expressions.$;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/2/22 15:26
 */
public class StreamToTableDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        //将list数据转换为datastream
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(new ArrayList<String>() {{
            add("hello,world");
            add("hello,chaixiaoxue");
        }});
        //将数据进行切分
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        Table cTable = tableEnvironment.fromDataStream(stringSingleOutputStreamOperator,$("name"));
        //将cTable 注册成一张表，供下面sql查询使用
        tableEnvironment.createTemporaryView("cTable",cTable);
        CloseableIterator<Row> collect = tableEnvironment.executeSql("select * from cTable").collect();
        while (collect.hasNext()){
            Row next = collect.next();
            System.out.println(next.toString());
        }
        //下面是第二种方式
        /*Table select = cTable.select($("name"));
        DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(select, Row.class);*/
        /*rowDataStream.print();
        env.execute("job staream to table");*/


    }
}
