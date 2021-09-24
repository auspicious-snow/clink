package com.auspicious.snow.clink.stream.clickhouse;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/9/16 15:31
 */
public class KafkaToClickHouseProd {

    public static void main(String[] args) {

      /*  if (args.length < 1){
            return;
        }
        //{"tableName"}

        JSONObject jsonObject = JSON.parseObject(args[1]);*/

        String sourceDDL = "CREATE TABLE sc_bill_retailordergoods_dt (\n" +
            " `BillDtlID` string   ,\n"
            + "    `BillID` string      ,\n"
            + "    `orgCode` string   ,\n"
            + "    `goodsId` string     ,\n"
            + "    `pf_goodsId` string  , \n"
            + "    `goodsCode` string   ,\n"
            + "    `goodsName` string   ,\n"
            + "    `generalName` string ,\n"
            + "    `goodsSpec` string   ,\n"
            + "    `unit` int           ,\n"
            + "    `manufacturer` string,\n"
            + "    `place` string       ,\n"
            + "    `formula` int        ,\n"
            + "     `createTime` timestamp,\n"
            + "     `updateTime` timestamp"+
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
            " `billdtlid` string   ,\n"
            + "    `billid` string      ,\n"
            + "    `ordercode` string   ,\n"
            + "    `goodsid` string     ,\n"
            + "    `pf_goodsid` string  , \n"
            + "    `goodscode` string   ,\n"
            + "    `goodsname` string   ,\n"
            + "    `generalname` string ,\n"
            + "    `goodsspec` string   ,\n"
            + "    `unit` int           ,\n"
            + "    `manufacturer` string,\n"
            + "    `place` string       ,\n"
            + "    `formula` int        ,PRIMARY KEY(billdtlid,billid) NOT ENFORCED"+
 //           + "     `createTime` timestamp,\n"
 //           + "     `updateTime` timestamp"+
            ") WITH (\n" +
            "           'connector' = 'clickhouse',  \n"
            + "        'url' = 'clickhouse://192.168.91.10:8123',\n"
            + "        'username' = 'default',  \n"
            + "        'password' = 'r1dd16c1a1980475',  \n"
            + "        'database-name' = 'tsetkafkadb', \n"
            + "        'table-name' = 'test_sc_bill_retailordergoods',  "+
            "           'sink.batch-size' = '1000', \n"
            + "        'sink.flush-interval' = '1s',\n"
            + "        'sink.max-retries' = '3',\n"
            + "        'sink.ignore-delete' = 'false'"+
//            "           'sink.write-local' = 'true'"+
            ")";
        String sqlDml = "insert into t2 select " +
            " `BillDtlID`,\n"
            + "`BillID`,\n"
            + "`orgCode`,\n"
            + "`goodsId`,\n"
            + "`pf_goodsId`, \n"
            + "`goodsCode`,\n"
            + "`goodsName`,\n"
            + "`generalName`,\n"
            + "`goodsSpec`,\n"
            + "`unit`,\n"
            + "`manufacturer`,\n"
            + "`place`,\n"
            + "`formula`\n"+
 //           "createTime,"+
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
