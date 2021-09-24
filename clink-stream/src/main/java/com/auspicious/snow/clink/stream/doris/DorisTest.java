package com.auspicious.snow.clink.stream.doris;

import com.dorisdb.connector.flink.DorisSink;
import com.dorisdb.connector.flink.table.DorisSinkOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/4/12 15:45
 */
public class DorisTest {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*System.setProperty("HADOOP_USER_NAME", "hive");
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
        config.setMaxConcurrentCheckpoints(1);*/

        env.fromElements(new String[]{
            "{\"id\":1, \"name\": \"stephen\",\"amount\": \"99\"}",
            "{\"id\":2, \"name\": \"lebron\",\"amount\": \"100\"}"
        }).addSink(DorisSink.sink(// the sink options
            DorisSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://192.168.3.102:9030")
                .withProperty("load-url", "192.168.3.102:8030")
                .withProperty("username", "test")
                .withProperty("password", "123456")
                .withProperty("table-name", "table1")
                .withProperty("database-name", "cxx_db")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build()));
        env.execute("doris demo");
    }
}
