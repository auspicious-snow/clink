package com.auspicious.snow.state.checkpoint.metadata;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/3/3 14:55
 */
public class CheckpointMetadataInfoByFile {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "file:////tmp/backend/86dcc2aa3af5fa8507b6bb8c0922ee5d/chk-3/", new MemoryStateBackend());
        DataSet<Tuple2<KafkaTopicPartition, Long>> haha = savepoint.readUnionState("haha", "topic-partition-offset-states",
            TypeInformation.of(new TypeHint<Tuple2<KafkaTopicPartition, Long>>() {}));
        haha.printOnTaskManager("source");
        //haha.print();
        bEnv.execute("aa");
    }
}
