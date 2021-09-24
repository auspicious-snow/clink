package com.auspicious.snow.clink.stream.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/9/8 10:24
 */
public class CanalJsonTestProd {

    public static void main(String[] args) {
        String sourceDDL = "CREATE TABLE mysqlTable (\n" +
            "  BillDtlID String, \n" +
            "  BillID string,"+
            "  createTime time,proc AS PROCTIME() \n" +
            ") WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'supplychain',\n" +
            " 'properties.bootstrap.servers' = '192.168.1.111:9092',\n" +
            " 'properties.group.id' = 'cxx',\n" +
            " 'scan.startup.mode' = 'timestamp',\n" +
            "'scan.startup.timestamp-millis' = '1631635200000',"+
            " 'format' = 'canal-json',\n" +
            " 'canal-json.ignore-parse-errors' = 'true',\n" +
            " 'canal-json.database.include' = 'SupplyChain',\n"+
            " 'canal-json.table.include' = 'sc_bill_retailordergoods_dt'"+
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

            "  BillDtlID string"
            + ",BillID String"+
            ") WITH (\n" +
            "   'connector' = 'print'"+
            ")";
        /*String joinSql = " insert into t2 "
            + " SELECT "
            + "        w.rowkey"
            + "        ,mysqlTable.id"
            + "        ,w.info.user_sex as sex"
            + "        ,w.info.user_age as age "
            + "        ,mysqlTable.name"
            + " FROM mysqlTable\n"
            + " LEFT JOIN hTable FOR SYSTEM_TIME AS OF mysqlTable.proc as w\n"
            + " ON cast(mysqlTable.id as string) = w.rowkey ";*/
        String sqlDml = "insert into t2" +
            " SELECT BillDtlID,BillID " +
            " FROM mysqlTable " +
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
