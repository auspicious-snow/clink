package com.auspicious.snow.clink.stream.kafka;

import static com.auspicious.snow.clink.common.PropertiesConstants.*;

import com.auspicious.snow.clink.common.PropertiesConstants;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/2/22 17:40
 */
public class KafkaUtils {


    public static Properties buildKafkaConfig(ParameterTool parameterTool){
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.FLINK_KAFKA_BOOTSTRAP_SERVERS, FLINK_KAFKA_BOOTSTRAP_SERVERS_DEFAULT));
        props.put("group.id", parameterTool.get(FLINK_KAFKA_GROUP_ID, FLINK_KAFKA_GROUP_ID_DEFAULT));
        return props;
    }
    public static FlinkKafkaConsumer getFlinkKafkaConsumer (String topic,StartupMode startupMode, Properties properties){
        return getFlinkKafkaConsumer(topic,new SimpleStringSchema(),startupMode,properties);
    }

    public static FlinkKafkaConsumer getFlinkKafkaConsumer (String topic,DeserializationSchema deserializationSchema,StartupMode startupMode, Properties properties){
        return getFlinkKafkaConsumer(topic,deserializationSchema,startupMode,properties,0L);
    }
    public static FlinkKafkaConsumer getFlinkKafkaConsumer (String topic, DeserializationSchema deserializationSchema,StartupMode startupMode, Properties properties,Long startTime){
        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer(topic,deserializationSchema,properties);
        //根据不同的消费策略进行消费kafka
        switch (startupMode) {
            case EARLIEST:
                flinkKafkaConsumer.setStartFromEarliest();
                break;
            case LATEST:
                flinkKafkaConsumer.setStartFromLatest();
                break;
            case GROUP_OFFSETS:
                flinkKafkaConsumer.setStartFromGroupOffsets();
                break;
            case TIMESTAMP:
                flinkKafkaConsumer.setStartFromTimestamp(startTime);
        }
        return flinkKafkaConsumer;
    }

}
