package com.quspicious.snow.clink.files;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/3/12 15:31
 */
public class FileSystemConnector {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,settings);
        CheckpointConfig config = env.getCheckpointConfig();
        env.enableCheckpointing(100);
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
        //hdfs://192.168.3.100:8020/user/hive/warehouse/topic_hive
        //file:////tmp/backend'


        streamTableEnvironment.executeSql("CREATE TABLE goods_info_df2 (\n"
            + "  name STRING,\n"
            + "  message string,\n"
            + "  dt string,"
            + "  minit string\n"
            + ") "
            + "PARTITIONED BY (dt, minit) "
            + "WITH (\n"
            //+"  'connector'='print'\n"
            + "  'connector'='filesystem',\n"
            + "  'path'='hdfs://192.168.3.101:8020/user/hive/warehouse/topic_hive',\n"
            + "  'format'='orc', "
            //+"'sink.rolling-policy.file-size' = '100m',\n"
            //+ "    'sink.rolling-policy.rollover-interval' = '10 s',\n"
            //+ "    'sink.rolling-policy.check-interval' = '1 s',\n"
            + "    'auto-compaction'='true',\n"
            //+ "   'sink.partition-commit.trigger'='partition-time',\n"
            //+ "  'sink.partition-commit.delay'='1 s',"
           // + "  'partition.time-extractor.timestamp-pattern'='$dt,$minit',\n"
            + "  'sink.partition-commit.policy.kind'='success-file'\n"
            //+ "  'sink.partition-commit.success-file.name'='_SUCCESS'\n"
            + ")");
        //1615538762997
        //1615538762000
        streamTableEnvironment.executeSql("CREATE TABLE ck (\n"
            + "  batch_id string,\n"
            + "  message string,\n"
            + "  `timestamp` timestamp(3),\n"
            + "  md5 string,\n"
            + "  es  bigint,"
            + "  ts AS TO_TIMESTAMP(FROM_UNIXTIME(es, 'yyyy-MM-dd HH:mm:ss')),"
            + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
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
            + "message"
            + ",DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'mm')\n"
            + "FROM ck");
    }
}
