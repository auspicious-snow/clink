package com.auspicious.snow.clink.stream.hbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/9/8 15:18
 */
public class HbaseSource {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        String sql = "CREATE TABLE hTable (\n"
            + " rowkey string,\n"
            + " info ROW<user_id string,user_age string,user_sex string>,\n"
            + " PRIMARY KEY (rowkey) NOT ENFORCED\n"
            + ") WITH (\n"
            + " 'connector' = 'hbase-2.2',\n"
            + " 'table-name' = 'user_info',\n"
            + " 'zookeeper.quorum' = '192.168.3.20:2181,192.168.3.21:2181,192.168.3.22:2181,192.168.3.23:2181,192.168.3.20:2181,192.168.3.24:2181'\n"
            + ")";
        //String sql2 = "select rowkey,info,info.user_id,info.user_age,info.user_sex from hTable";

        String hbaseSinkDDL = "CREATE TABLE t2 (\n" +

            "  rowkey string ,user_id string" +
            ") WITH (\n" +
            "   'connector' = 'print'"+
            ")";
        String sqlDml = "insert into t2" +
            " SELECT rowkey,info.user_id " +
            " FROM hTable " +
            "";
        streamTableEnvironment.executeSql(sql);
        streamTableEnvironment.executeSql(hbaseSinkDDL);
        streamTableEnvironment.executeSql(sqlDml);


    }

}
