package com.auspicious.snow.clink.stream.hive;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ Description
 * @ Author huangsheng
 * @ Create 2021-03-31 14:02
 */
public class KafkaToHiveByFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME", "hive");
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

        // 1.创建blink流式表环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        // 2.创建、注册并使用hive catalog
        String name            = "myhive";
        String defaultDatabase = "tmp";
        String hiveConfDir     = "D:\\github\\clink\\clink-stream\\src\\main\\resources";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        bsTableEnv.registerCatalog("myhive", hive);
        bsTableEnv.useCatalog("myhive");

        //设置hive方言，也就是让flink支持hive的sql语法
        bsTableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        //3.基于hive创建输出表
        bsTableEnv.executeSql(
                "CREATE TABLE if not exists hive_mall_order_info (\n" +
                        " `batch_id`   string,\n" +
                        " `message`    string,\n" +
                        " `event_time` string,\n" +
                        " `md5`        string,\n" +
                        " `es`         string,\n" +
                        " `ts`         string \n" +
                        ") PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt',\n" +
                        "  'sink.partition-commit.trigger'='partition-time',\n" +
                        "  'sink.partition-commit.delay'='1 h',\n" +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                        ")"
        );

        //将方言切换成default
        bsTableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // 4.基于kafka创建输入表
        bsTableEnv.executeSql(
                "CREATE TABLE if not exists kafka_mall_order_info (\n" +
                   " `batch_id`   string,\n" +
                   " `message`    string,\n" +
                   " `event_time` string,\n" +
                   " `md5`        string,\n" +
                   " `es`         string,\n" +
                   " `ts`         string" +
                   "  ) WITH (\n" +
                   " 'connector' = 'kafka',\n" +
                   " 'topic' = 'yx-Oms-mall_order_info-topic',\n" +
                   " 'properties.bootstrap.servers' = '192.168.3.102:9092',\n" +
                   " 'properties.group.id' = 'kafka_to_hive',\n" +
                   " 'scan.startup.mode' = 'latest-offset',\n" +
                   " 'format' = 'json',\n" +
                   " 'json.fail-on-missing-field' = 'false',\n" +
                   " 'json.ignore-parse-errors' = 'true'\n" +
                   ")"
        );

        Table table = bsTableEnv.sqlQuery(
                   "SELECT batch_id,message,event_time,md5,es,ts,DATE_FORMAT(event_time, 'yyyy-MM-dd') FROM kafka_mall_order_info"
        );

        // 5.触发，将kafka中的数据插入hive
        TableResult tableResult = table.executeInsert("hive_mall_order_info");
        //bsTableEnv.toAppendStream(table,Row.class).print();
        tableResult.print();
        env.execute();
    }
}
