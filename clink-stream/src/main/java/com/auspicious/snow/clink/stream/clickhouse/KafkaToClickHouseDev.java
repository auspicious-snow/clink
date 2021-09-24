package com.auspicious.snow.clink.stream.clickhouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/9/16 15:31
 */
public class KafkaToClickHouseDev {

    public static void main(String[] args) {

      /*  if (args.length < 1){
            return;
        }
        //{"tableName"}

        JSONObject jsonObject = JSON.parseObject(args[1]);*/

        String sourceDDL = "CREATE TABLE sc_bill_retailordergoods_dt (\n" +
            " `id` int   ,\n"
            + "    `mem_name` string      ,\n"
            + "    `mem_id` int   ,\n"
            + "    `bill_type` string     ,\n"
            + "     `update_time` timestamp,ts AS PROCTIME()"+
            ") WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'topic_yx-dc-3-102_3306',\n" +
            " 'properties.bootstrap.servers' = '192.168.3.100:9092',\n" +
            " 'properties.group.id' = 'cxx',\n" +
            " 'scan.startup.mode' = 'latest-offset',\n" +
            //"'scan.startup.timestamp-millis' = '1631635200000',"+
            " 'format' = 'canal-json',\n" +
            " 'canal-json.ignore-parse-errors' = 'true',\n" +
            " 'canal-json.database.include' = 'chaixiaoxue',\n"+
            " 'canal-json.table.include' = 'bill_retailorder'"+
            ")";

        //String sinkDDL = "create table t2 (message string,u_utrace string ,ts timestamp(3) ) with ('connector' = 'print')"; 1631698561943

        /*String sql = "CREATE TABLE hTable (\n"
            + " rowkey string,\n"
            + " info ROW<user_id string,user_age string,user_sex string>,\n"
            + " PRIMARY KEY (rowkey) NOT ENFORCED\n"
            + ") WITH (\n"
            + " 'connector' = 'hbase-2.2',\n"
            + " 'table-name' = 'user_info',\n"
            + " 'zookeeper.quorum' = '192.168.3.20:2181,192.168.3.21:2181,192.168.3.22:2181,192.168.3.23:2181,192.168.3.20:2181,192.168.3.24:2181'\n"
            + ")";*/


        String mysqlSinkDDL = "CREATE TABLE t2 (\n" +
            " `id` int   ,\n"
            + "    `mem_name` string      ,\n"
            + "    `mem_id` int   ,\n"
            + "    `bill_type` string     ,\n"
            + "     `update_time` timestamp,ts timestamp,"
            + "    PRIMARY KEY(id,mem_name) NOT ENFORCED"+
 //           + "     `createTime` timestamp,\n"
 //           + "     `updateTime` timestamp"+
            ") WITH (\n" +
            "           'connector' = 'clickhouse',  \n"
            + "        'url' = 'clickhouse://192.168.91.10:8123',\n"
            + "        'username' = 'default',  \n"
            + "        'password' = 'r1dd16c1a1980475',  \n"
            + "        'database-name' = 'tsetkafkadb', \n"
            + "        'table-name' = 'test_bill_retailorder',  "+
            "           'sink.batch-size' = '1', \n"
            + "        'sink.flush-interval' = '1s',\n"
            + "        'sink.max-retries' = '3',\n"
            + "        'sink.ignore-delete' = 'true'"+
//            "           'sink.write-local' = 'true'"+
            ")";
        String sqlDml = "insert into t2 select " +
            " `id`,\n"
            + "`mem_name`,\n"
            + "`mem_id`,\n"
            + "`bill_type`,\n"
            + "`update_time`, \n"+
            "ts"+
 //           "updateTime"+
            " FROM sc_bill_retailordergoods_dt " +
            "";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        streamTableEnvironment.executeSql(sourceDDL);
        streamTableEnvironment.executeSql(mysqlSinkDDL);
        streamTableEnvironment.executeSql(sqlDml);



    }

}
