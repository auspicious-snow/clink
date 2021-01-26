# clink
##clink-format
&nbsp;&nbsp;&nbsp;&nbsp;该模块是自定义的flink-format，用来实现特殊的canal-json格式的解析。
我们的canal-json格式如下(其中新增了batch_id,md5,timestamp)：
```json
{"batch_id":"1","time":"2021-01-25 17:23:19.596","message":"{\"data\":[{\"last_modify_time\":\"2021-01-25 17:23:16\",\"id\":\"91\"}],\"pkNames\":[\"id\"],\"type\":\"INSERT\",\"es\":1611566599000,\"sql\":\"\",\"database\":\"Oms\",\"sqlType\":{\"last_modify_time\":93,\"id\":-5},\"mysqlType\":{\"last_modify_time\":\"timestamp\",\"id\":\"bigint(20) unsigned\"},\"id\":95,\"isDdl\":false,\"table\":\"mall_order_info\",\"ts\":1611566599596}","md5":"3194cfff95a987fe3cd003b99b59a855","timestamp":1611566599596}
```
&nbsp;&nbsp;&nbsp;&nbsp;所以我们再次消费的时候就不能使用flink自带的原生canal-json。   
通过自定义dml-json-format 将我们需要的字段也添加到我们可以指定列进行消费   
消费方式如下：
```java
public class FlinkKafkaSourceMyFormat {
streamTableEnvironment.executeSql("CREATE TABLE topic_products (\n" +
                "  last_modify_time string, " +
                " id int," +
                "info ROW<batch_id string,md5 STRING>\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'cxx',\n" +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'dml-json'  \n" +
                ")");
}
```




