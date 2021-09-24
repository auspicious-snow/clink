package com.auspicious.snow.clink.stream.cdc;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/4/25 14:31
 */
public class MysqlCdcDemo {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        streamTableEnvironment.executeSql("CREATE TABLE mysql_binlog "
            + "(id INT NOT NULL, gender STRING, amount double)"
            + " WITH ('connector' = 'mysql-cdc', "
            + "'hostname' = '192.168.3.102', 'port' = '3306', "
            + "'username' = 'admin', 'password' = 'LFo7AWol&dCM^8eH', "
            + "'database-name' = 'Oms', 'table-name' = 'test_sum')\n"
           );

        streamTableEnvironment.executeSql("CREATE TABLE sink_table ( \n"
            + "gender string,\n"
            + "amount double,\n"
            + "PRIMARY KEY (gender) NOT ENFORCED \n"
            + ") \n"
            + "   WITH (\n"
            + "     'connector'='redis'"
            + ",\n"
            + "     'host' = '192.168.3.21',\n"
            + "     'port'= '6379',\n"
            + "     'redis-mode'='single',\n"
            + "     'command'='hset',\n"
            + "     'additional-key'='test_gender'\n"
            + "\t)");
        streamTableEnvironment.executeSql("INSERT INTO sink_table SELECT  'M' as gender, sum(amount) FROM mysql_binlog group by gender");

    }

}
