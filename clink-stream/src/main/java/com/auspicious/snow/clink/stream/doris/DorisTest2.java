package com.auspicious.snow.clink.stream.doris;

import com.dorisdb.connector.flink.DorisSink;
import com.dorisdb.connector.flink.table.DorisSinkOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

/**
 * @author chaixiaoxue
 * @version 1.0
 * @date 2021/4/12 15:45
 */
public class DorisTest2 {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.fromElements(
            new RowData[]{
                new RowData(3,99D, "stephen"),
                new RowData(4,100D, "lebron")
            }
        ).addSink(
            DorisSink.sink(
                // the table structure
                TableSchema.builder()
                    .field("id",DataTypes.INT())
                    .field("name", DataTypes.VARCHAR(20))
                    .field("amount", DataTypes.DOUBLE())
                    .build(),
                // the sink options
                DorisSinkOptions.builder()
                    .withProperty("jdbc-url", "jdbc:mysql://192.168.3.102:9030")
                    .withProperty("load-url", "192.168.3.102:8030")
                    .withProperty("username", "test")
                    .withProperty("password", "123456")
                    .withProperty("table-name", "table1")
                    .withProperty("database-name", "cxx_db")
                    .build(),
                // set the slots with streamRowData
                (slots, streamRowData) -> {
                    slots[0] = streamRowData.id;
                    slots[2] = streamRowData.amount*10;
                    slots[1] = streamRowData.name;
                }
            )
        );
        env.execute("doris demo");
    }
}
