package com.auspicious.snow.clink.stream.redis;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/4/20 18:24
 */
public class TestRedisSink {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();*/
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        //streamTableEnvironment.getConfig().getConfiguration().setString("table.exec.source.cdc-events-duplicate","true");
        streamTableEnvironment.executeSql("CREATE TABLE BIG_SCREEN ( \n"
            + "    INSURANCE_ID String, \n"
            + "    PAY_STATUS String, \n"
            + "    NUMBERS int, \n"
           // + "    INSURED_FEE_AMOUNT DOUBLE, \n"
            + "    PAY_TYPE String \n"

            + "\t) WITH ( \n"
            + "    'connector' = 'mysql-cdc', \n"
            + "'hostname' = 'rr-uf6qa11lts4i846es.mysql.rds.aliyuncs.com', 'port' = '3306', "
            + "'username' = 'jinchenghuibao', 'password' = 'Fl5dWBe6IlIT1Ogi', "
            + "'database-name' = 'insurance_core_jinchenghuibao', 'table-name' = 'if_user_insure'\n"
            + "\t)");

        streamTableEnvironment.executeSql("CREATE TABLE big_screen_person_cnt ( \n"
            + "      PAY_DATE string,\n"
            + "insurance_person_cn int\n"
            + "      ,\n"
            + "      PRIMARY KEY (PAY_DATE) NOT ENFORCED \n"
            + "\t) WITH (\n"
            + "      'connector'='print'"
            /*+ ",\n"
            + "      'host' = '192.168.3.21',\n"
            + "      'port'= '6379',\n"
            + "      'redis-mode'='single',\n"
            + "      'command'='hset',\n"
            + "      'additional-key'='yxhb-lz'\n"*/
            + "\t)");
        //cast(pay_date as string) as
        streamTableEnvironment.executeSql("INSERT INTO big_screen_person_cnt\n"
            + "SELECT\n"
            + " 'aaaa' as PAY_DATE,\n"
            + "  SUM(NUMBERS) AS insurance_person_cn\n"
            + "FROM BIG_SCREEN\n"
            + "WHERE  PAY_STATUS in ('payYet', 'partRefund', 'selfAccSucess') ");
            //+ "group by PAY_DATE");

        /*streamTableEnvironment.executeSql("CREATE TABLE kafka_mall_order_info (\n" +
            "  insurance_id String,\n" +
            "  NUMBERS double," +
            "PAY_TYPE string ," +
            "info ROW<batch_id string,md5 STRING>,\n" +
            "   PRIMARY KEY (insurance_id) NOT ENFORCED\n" +
            "   ) WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'yx-Oms-if_user_insure-topic',\n" +
            "  'properties.bootstrap.servers' = '192.168.3.100:9092,192.168.3.101:9092,192.168.3.102:9092',\n" +
            "  'properties.group.id' = 'kafka_to_hive',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'format' = 'dml-json')");

        streamTableEnvironment.executeSql("create table t2 (gender string,amount double) with ('connector' = 'print')");
        streamTableEnvironment.executeSql("insert into t2 select PAY_TYPE,sum(NUMBERS) from kafka_mall_order_info group by PAY_TYPE");*/
    }

}
