package io.github.homepy.whale.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class BlinkTableJobDemo {

    public static void main(String[] args) {

        // **********************
// BLINK STREAMING QUERY
// **********************

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);


        // Virtual Tables
        // table is the result of a simple projection query
        /*
        Table projTable = tableEnv.from("X").select("...");
        tableEnv.createTemporaryView("Orders", projTable);
        */

        // Connector Tables
        /*
        tableEnv.connect(...).withFormat(...).withSchema(...).inAppendMode().createTemporaryTable("Orders");
         */


        Table revenue = tableEnv.sqlQuery(
                "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );

        tableEnv.executeSql(
                "INSERT INTO RevenueFrance " +
                        "SELECT cID, cName, SUM(revenue) AS revSum " +
                        "FROM Orders " +
                        "WHERE cCountry = 'FRANCE' " +
                        "GROUP BY cID, cName"
        );


        // create an output Table
        final Schema schema = new Schema()
                .field("a", DataTypes.INT())
                .field("b", DataTypes.STRING())
                .field("c", DataTypes.BIGINT());

//        tableEnv.connect(new FileSystem().path("/path/to/file"))
//                .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
//                .withSchema(schema)
//                .createTemporaryTable("CsvSinkTable");

        // emit the result Table to the registered TableSink
        revenue.executeInsert("CsvSinkTable");

    }
}
