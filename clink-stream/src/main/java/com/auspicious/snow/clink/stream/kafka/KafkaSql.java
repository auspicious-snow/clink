package com.auspicious.snow.clink.stream.kafka;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/4/14 10:36
 */
public class KafkaSql {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env,settings);

        bsTableEnv.executeSql("CREATE TABLE kafka_mall_order_info (\n" +
            "  last_modify_time string, " +
            " id int, " +
            "info ROW<batch_id string,md5 STRING>\n" +
            "  ) WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'yx-Oms-if_user_insure-topic',\n" +
            " 'properties.bootstrap.servers' = '192.168.3.100:9092,192.168.3.101:9092,192.168.3.102:9092',\n" +
            " 'properties.group.id' = 'kafka_to_hive',\n" +
            " 'scan.startup.mode' = 'latest-offset',\n" +
            " 'format' = 'dml-json'\n" +

            ")");

        bsTableEnv.executeSql("create table t2 (id  int) with ('connector' = 'print')");
        bsTableEnv.executeSql("insert into t2 select id from kafka_mall_order_info");



    }

}
