package com.auspicious.snow.clink.stream.hive;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/3/11 14:56
 */
public class HiveCreateTable {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        CheckpointConfig config = env.getCheckpointConfig();
        env.enableCheckpointing(100);
        System.setProperty("HADOOP_USER_NAME", "hive");

        env.getCheckpointConfig().setCheckpointInterval(15000);
        //env.setStateBackend(new FsStateBackend("file:////tmp/backend"));
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置模式为exactly-once
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setCheckpointInterval(1000);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        config.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        config.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        config.setMaxConcurrentCheckpoints(1);
        //D:\github\clink\clink-stream\src\main\resources\
        streamTableEnvironment.executeSql("CREATE TABLE goods_info_df2 (\n"
            + "  name STRING,\n"
            + "  age string,"
            + "  dt string,"
            + "  h string\n"
            + ") PARTITIONED BY (dt,h) WITH (\n"
            + "  'connector' = 'hive',"
            + "'hive.version'='2.1.1',"
            + "'hive.table'='goods_info_df7',"
            + "'hive.database'='tmp',"
            +"    'hive.conf.dir'='/etc/hive/conf',"
            + "  'sink.partition-commit.policy.kind'='metastore,success-file'\n"
            + ")");

        streamTableEnvironment.executeSql("CREATE TABLE ck (\n"
            + "  batch_id string,\n"
            + "  message string,\n"
            + "  `timestamp` bigint,\n"
            + "  md5 string,\n"
            + "  es bigint,\n"
            +"   ts AS TO_TIMESTAMP(FROM_UNIXTIME(es, 'yyyy-MM-dd HH:mm:ss'))   "
            + "  ) WITH (\n"
            + " 'connector' = 'kafka',\n"
            + " 'topic' = 'yx-Oms-mall_order_info-topic',\n"
            + " 'properties.bootstrap.servers' = '192.168.3.102:9092',\n"
            + " 'properties.group.id' = 'kafka_to_hive',\n"
            + " 'scan.startup.mode' = 'group-offsets',\n"
            + " 'format' = 'json',\n"
            + " 'json.fail-on-missing-field' = 'false',\n"
            + " 'json.ignore-parse-errors' = 'true'\n"
            + ")");

        streamTableEnvironment.executeSql("insert into goods_info_df2 \n"
            + "SELECT \n"
            + "batch_id,\n"
            + "message,DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH')\n"
            + "FROM ck");
    }
}
