package io.github.homepy.whale.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

public class KafkaConnector {

    public void source (StreamTableEnvironment tableEnvironment) {

        tableEnvironment
                // declare the external system to connect to
                .connect(
                        new Kafka()
                                .version("0.10")
                                .topic("test-input")
                                .startFromEarliest()
                                .property("bootstrap.servers", "localhost:9092")
                )

                // declare a format for this system
                .withFormat(
                        new Json()
                )

                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("rowtime", DataTypes.TIMESTAMP(3))
                                .rowtime(new Rowtime()
                                        .timestampsFromField("timestamp")
                                        .watermarksPeriodicBounded(60000)
                                )
                                .field("user", DataTypes.BIGINT())
                                .field("message", DataTypes.STRING())
                )

                // create a table with given name
                .createTemporaryTable("MyUserTable");
    }
}
