package com.auspicious.snow.clink.stream.kafka;

import com.auspicious.snow.clink.common.PropertiesConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/2/22 17:30
 * 读取kafka数据
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {
        //创建stream的环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认的并行度是和cpu的核数一样
        env.setParallelism(1);
        Map<String, String> pro = new HashMap();
        pro.put(PropertiesConstants.FLINK_KAFKA_BOOTSTRAP_SERVERS,"192.168.1.111:9092");
        pro.put(PropertiesConstants.FLINK_KAFKA_GROUP_ID,"clink");
        ParameterTool parameterTool = ParameterTool.fromMap(pro);
        Properties properties = KafkaUtils.buildKafkaConfig(parameterTool);
        //获取kafkaConsumer
        FlinkKafkaConsumer kafkaConsumer = KafkaUtils.getFlinkKafkaConsumer("snow",new SimpleStringSchema(), StartupMode.TIMESTAMP, properties,1631635200000L);
        DataStreamSource source = env.addSource(kafkaConsumer);
        source.print();
        env.execute("kafka source");
    }
}
