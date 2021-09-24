package com.auspicious.snow.clink.stream.hive;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/3/11 10:56
 */
public class HiveSqlParse {

    public static SqlParser.Config sqlParserConfig = SqlParser
        .configBuilder()
        .setParserFactory(FlinkSqlParserImpl.FACTORY)
        .setConformance(FlinkSqlConformance.HIVE)
        .setLex(Lex.JAVA)
        .setIdentifierMaxLength(256)
        .build();
    public static void main(String[] args) {
        String sql= "SET `table.sql-dialect`=hive;\n"
            + "CREATE TABLE tmp.hive_table (\n"
            + "  batch_id STRING,\n"
            + "  message DOUBLE\n"
            + ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n"
            + "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n"
            + "  'sink.partition-commit.trigger'='partition-time',\n"
            + "  'sink.partition-commit.delay'='1 h',\n"
            + "  'sink.partition-commit.policy.kind'='metastore,success-file'\n"
            + ");";
        StreamExecutionEnvironment env = new LocalStreamEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironmentImpl tEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);
        try {
            SqlParser sqlParser = SqlParser.create(sql, sqlParserConfig);
            SqlNodeList sqlNodes = sqlParser.parseStmtList();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
