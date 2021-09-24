package com.auspicious.snow.clink.stream.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/9/8 10:24
 */
public class CanalJsonTest {

    public static void main(String[] args) {
        String sourceDDL = "CREATE TABLE mysqlTable (\n" +
            "  id int, \n" +
            "  name string,"+
            "  update_time TIMESTAMP(3),"
            + "proc AS PROCTIME(),WATERMARK FOR update_time AS update_time - INTERVAL '10' SECOND  \n" +
            ") WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'topic_yx-dc-3-102_3306',\n" +
            " 'properties.bootstrap.servers' = '192.168.3.102:9092',\n" +
            " 'properties.group.id' = 'cxx',\n" +
            " 'scan.startup.mode' = 'latest-offset',\n" +
            " 'format' = 'canal-json',\n" +
            " 'canal-json.ignore-parse-errors' = 'true',\n" +
            " 'canal-json.database.include' = 'chaixiaoxue',\n"+
            " 'canal-json.table.include' = 'user'"+
            ")";

        //String sinkDDL = "create table t2 (message string,u_utrace string ,ts timestamp(3) ) with ('connector' = 'print')";

        String sql = "CREATE TABLE hTable (\n"
            + " rowkey string,\n"
            + " info ROW<user_id string,user_age string,user_sex string>,update_time AS now(),\n"
            + " PRIMARY KEY (rowkey) NOT ENFORCED"
  //          + ",WATERMARK FOR update_time AS update_time - INTERVAL '10' SECOND\n"
            + ") WITH (\n"
            + " 'connector' = 'hbase-2.2',\n"
            + " 'table-name' = 'user_info',\n"
            + " 'zookeeper.quorum' = '192.168.3.20:2181,192.168.3.21:2181,192.168.3.22:2181,192.168.3.23:2181,192.168.3.20:2181,192.168.3.24:2181'\n"
            + ")";


        String mysqlSinkDDL = "CREATE TABLE t2 (\n" +

            "  rowkey string"
            + ",id int"
            + ",sex string"
            + ",age string"
            + ",name string" +
            ") WITH (\n" +
            "   'connector' = 'print'"+
            ")";
        String joinSql = " insert into t2 "
            + " SELECT "
            + "        w.rowkey"
            + "        ,mysqlTable.id"
            + "        ,w.info.user_sex as sex"
            + "        ,w.info.user_age as age "
            + "        ,mysqlTable.name"
            + " FROM mysqlTable\n"
            + " LEFT JOIN hTable FOR SYSTEM_TIME AS OF mysqlTable.update_time as w\n"
            + " ON cast(mysqlTable.id as string) = w.rowkey ";
        /*String sqlDml = "insert into t2" +
            " SELECT name " +
            " FROM mysqlTable " +
            "";*/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        streamTableEnvironment.executeSql(sourceDDL);
        streamTableEnvironment.executeSql(sql);
        streamTableEnvironment.executeSql(mysqlSinkDDL);
        streamTableEnvironment.executeSql(joinSql);
    }

}
