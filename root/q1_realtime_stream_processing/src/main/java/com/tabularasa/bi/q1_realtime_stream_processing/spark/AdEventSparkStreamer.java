package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class AdEventSparkStreamer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdEventSparkStreamer.class);
    private static final String TARGET_TABLE = "aggregated_campaign_stats";
    private static final StructType AD_EVENT_SCHEMA = new StructType()
            .add("timestamp", "timestamp")
            .add("campaign_id", "string")
            .add("event_type", "string")
            .add("user_id", "string")
            .add("spend_usd", "double")
            .add("device_type", "string")
            .add("country_code", "string")
            .add("product_brand", "string")
            .add("product_age_group", "string")
            .add("product_category_1", "integer")
            .add("product_category_2", "integer")
            .add("product_category_3", "integer")
            .add("product_category_4", "integer")
            .add("product_category_5", "integer")
            .add("product_category_6", "integer")
            .add("product_category_7", "integer")
            .add("product_price", "double")
            .add("sales_amount_euro", "double")
            .add("sale", "boolean");

    public static void main(String[] args) {
        // Basic argument parsing
        if (args.length < 4) {
            LOGGER.error("Usage: AdEventSparkStreamer <kafka-bootstrap-servers> <ad-events-topic> <db-url> <db-user> [<db-password>]");
            System.exit(1);
        }

        String kafkaBootstrapServers = args[0];
        String adEventsTopic = args[1];
        String dbUrl = args[2];
        String dbUsername = args[3];
        String dbPassword = args.length > 4 ? args[4] : ""; // Password can be empty for some setups

        SparkSession spark = SparkSession.builder()
                .appName("AdEventSparkStreamer")
                .getOrCreate();

        AdEventSparkStreamer streamer = new AdEventSparkStreamer();
        try {
            streamer.start(spark, kafkaBootstrapServers, adEventsTopic, dbUrl, dbUsername, dbPassword);
        } catch (TimeoutException | StreamingQueryException e) {
            LOGGER.error("Spark streaming job failed.", e);
            spark.stop();
        }
    }

    public void start(SparkSession spark, String kafkaBootstrapServers, String adEventsTopic,
                      String dbUrl, String dbUsername, String dbPassword) throws TimeoutException, StreamingQueryException {

        LOGGER.info("Initializing Spark Structured Streaming job for topic: {}", adEventsTopic);

        Dataset<Row> kafkaDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", adEventsTopic)
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> eventsDF = kafkaDF
                .select(from_json(col("value").cast("string"), AD_EVENT_SCHEMA).alias("event_data"))
                .select("event_data.*")
                .withColumn("event_timestamp", col("timestamp").cast("timestamp"));

        Dataset<Row> windowedDF = eventsDF
                .withWatermark("event_timestamp", "10 seconds")
                .groupBy(
                        window(col("event_timestamp"), "1 minute"),
                        col("campaign_id"),
                        col("event_type"),
                        col("device_type"),
                        col("country_code"),
                        col("product_brand"),
                        col("product_age_group"),
                        col("product_category_1"),
                        col("product_category_2"),
                        col("product_category_3"),
                        col("product_category_4"),
                        col("product_category_5"),
                        col("product_category_6"),
                        col("product_category_7")
                )
                .agg(
                        count("*").alias("event_count"),
                        sum("spend_usd").alias("total_spend_usd"),
                        sum("sales_amount_euro").alias("total_sales_amount_euro"),
                        sum(when(col("sale"), 1).otherwise(0)).alias("total_sales_count")
                );

        LOGGER.info("Starting streaming query with windowing to PostgreSQL...");

        StreamingQuery streamingQuery = windowedDF
                .writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {
                    LOGGER.info("Processing batch: {}", batchId);
                    batchDF.persist();

                    Dataset<Row> processedDF = batchDF
                            .withColumn("window_start_time", col("window.start"))
                            .drop("window");

                    processedDF.foreachPartition(partitionOfRecords -> {
                        Properties connectionProperties = new Properties();
                        connectionProperties.put("user", dbUsername);
                        connectionProperties.put("password", dbPassword);

                        String upsertSQL = String.format(
                                "INSERT INTO %s (campaign_id, event_type, window_start_time, device_type, country_code, " +
                                        "product_brand, product_age_group, product_category_1, product_category_2, " +
                                        "product_category_3, product_category_4, product_category_5, product_category_6, " +
                                        "product_category_7, event_count, total_spend_usd, total_sales_amount_euro, total_sales_count, updated_at) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                                        "ON CONFLICT (campaign_id, event_type, window_start_time, device_type, country_code, " +
                                        "product_brand, product_age_group, product_category_1, product_category_2, " +
                                        "product_category_3, product_category_4, product_category_5, product_category_6, " +
                                        "product_category_7) DO UPDATE SET " +
                                        "event_count = EXCLUDED.event_count, " +
                                        "total_spend_usd = EXCLUDED.total_spend_usd, " +
                                        "total_sales_amount_euro = EXCLUDED.total_sales_amount_euro, " +
                                        "total_sales_count = EXCLUDED.total_sales_count, " +
                                        "updated_at = CURRENT_TIMESTAMP",
                                TARGET_TABLE
                        );

                        try (Connection connection = DriverManager.getConnection(dbUrl, connectionProperties)) {
                            connection.setAutoCommit(false);
                            try (PreparedStatement statement = connection.prepareStatement(upsertSQL)) {
                                int count = 0;
                                while (partitionOfRecords.hasNext()) {
                                    Row record = partitionOfRecords.next();
                                    statement.setString(1, record.getString(record.fieldIndex("campaign_id")));
                                    statement.setString(2, record.getString(record.fieldIndex("event_type")));
                                    statement.setTimestamp(3, record.getTimestamp(record.fieldIndex("window_start_time")));
                                    statement.setString(4, record.getString(record.fieldIndex("device_type")));
                                    statement.setString(5, record.getString(record.fieldIndex("country_code")));
                                    statement.setString(6, record.getString(record.fieldIndex("product_brand")));
                                    statement.setString(7, record.getString(record.fieldIndex("product_age_group")));
                                    statement.setInt(8, record.getInt(record.fieldIndex("product_category_1")));
                                    statement.setInt(9, record.getInt(record.fieldIndex("product_category_2")));
                                    statement.setInt(10, record.getInt(record.fieldIndex("product_category_3")));
                                    statement.setInt(11, record.getInt(record.fieldIndex("product_category_4")));
                                    statement.setInt(12, record.getInt(record.fieldIndex("product_category_5")));
                                    statement.setInt(13, record.getInt(record.fieldIndex("product_category_6")));
                                    statement.setInt(14, record.getInt(record.fieldIndex("product_category_7")));
                                    statement.setLong(15, record.getLong(record.fieldIndex("event_count")));
                                    statement.setDouble(16, record.getDouble(record.fieldIndex("total_spend_usd")));
                                    statement.setDouble(17, record.getDouble(record.fieldIndex("total_sales_amount_euro")));
                                    statement.setLong(18, record.getLong(record.fieldIndex("total_sales_count")));
                                    statement.addBatch();
                                    count++;
                                    if (count % 1000 == 0) {
                                        statement.executeBatch();
                                        connection.commit();
                                        LOGGER.info("Committed {} records in partition.", count);
                                    }
                                }
                                statement.executeBatch();
                                connection.commit();
                                LOGGER.info("Finished processing partition, committed {} records.", count);
                            }
                        } catch (SQLException e) {
                            LOGGER.error("Error processing partition or writing to DB", e);
                            throw new RuntimeException("Error in foreachPartition", e);
                        }
                    });

                    batchDF.unpersist();
                })
                .option("checkpointLocation", "/tmp/spark-checkpoints/q1_ad_stream")
                .start();

        LOGGER.info("Spark Streaming job started. Query ID: {}", streamingQuery.id());
        streamingQuery.awaitTermination();
    }
}