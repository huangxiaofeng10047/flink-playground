package org.sample.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UpSertKafkaSQLJob {
    private SourceFunction<Long> source;
    private SinkFunction<Long> sink;

    public UpSertKafkaSQLJob(SourceFunction<Long> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    public UpSertKafkaSQLJob() {
    }

    public void execute() throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()        // access high-level configuration
                .getConfiguration()   // set low-level key-value options
                .setString("table.exec.resource.default-parallelism", String.valueOf(1));//set parallelism value equal to kafka partition of source topic

        tEnv.executeSql("CREATE TABLE pageviews_per_region (\n" +
                "  user_region STRING,\n" +
                "  pv BIGINT,\n" +
                "  uv BIGINT,\n" +
                "  PRIMARY KEY (user_region) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'pageviews_per_region',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ");");

        tEnv.executeSql("CREATE TABLE pageviews (\n" +
                "  user_id BIGINT,\n" +
                "  page_id BIGINT,\n" +
                "  viewtime TIMESTAMP(3),\n" +
                "  user_region STRING,\n" +
                "  WATERMARK FOR viewtime AS viewtime - INTERVAL '2' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'pageviews',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'format' = 'json'\n" +
                ");");

// -- calculate the pv, uv and insert into the upsert-kafka sink
        Table table2 = tEnv.sqlQuery("\n" +
                "SELECT\n" +
                "  user_region,\n" +
                "  COUNT(*),\n" +
                "  COUNT(DISTINCT user_id)\n" +
                "FROM pageviews\n" +
                "GROUP BY user_region;");


// Emit a Table API result Table to a TableSink, same for SQL result
        TableResult tableResult = table2.insertInto("pageviews_per_region").execute();

    }

    public static void main(String[] args) throws Exception {
        UpSertKafkaSQLJob job = new UpSertKafkaSQLJob();
        job.execute();
        /**发送数据为：
         *>{"orderId":"1","userId":"2","priceAmount":100.0,"eventTime":"2021-10-28 08:37:51.606","userName":"vg"}
         * TIMESTAMP(3) CURRENT_TIMESTAMP
         *
         * 入参说明
         * 无。
         *
         * 示例
         * 测试语句
         * SELECT
         * 	CURRENT_TIMESTAMP AS `result`
         * FROM
         * 	testtable;
         *
         * 测试结果
         * result
         *
         * 2021-10-28 08:33:51.606
         */
    }
}
