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
    
    // Define the base schema with bid_amount_usd field, as used in tests
    private static final StructType BASE_EVENT_SCHEMA = new StructType()
            .add("timestamp", "timestamp")
            .add("campaign_id", "string")
            .add("event_type", "string")
            .add("user_id", "string")
            .add("bid_amount_usd", "double");

    // Extended schema with additional fields that may be present in real data
    private static final StructType AD_EVENT_SCHEMA = BASE_EVENT_SCHEMA
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

        // Add a helper column `total_bid_amount` that is derived from bid_amount_usd so that
        // the column names exactly match the schema expected by the integration test
        Dataset<Row> enrichedDF = eventsDF
                .withColumn("total_bid_amount", coalesce(col("bid_amount_usd"), lit(0.0)))
                .withColumn("device_type", coalesce(col("device_type"), lit("unknown")))
                .withColumn("country_code", coalesce(col("country_code"), lit("unknown")))
                .withColumn("product_brand", coalesce(col("product_brand"), lit("unknown")))
                .withColumn("product_age_group", coalesce(col("product_age_group"), lit("unknown")))
                .withColumn("product_category_1", coalesce(col("product_category_1"), lit(0)))
                .withColumn("product_category_2", coalesce(col("product_category_2"), lit(0)))
                .withColumn("product_category_3", coalesce(col("product_category_3"), lit(0)))
                .withColumn("product_category_4", coalesce(col("product_category_4"), lit(0)))
                .withColumn("product_category_5", coalesce(col("product_category_5"), lit(0)))
                .withColumn("product_category_6", coalesce(col("product_category_6"), lit(0)))
                .withColumn("product_category_7", coalesce(col("product_category_7"), lit(0)))
                .withColumn("sales_amount_euro", coalesce(col("sales_amount_euro"), lit(0.0)))
                .withColumn("sale", coalesce(col("sale"), lit(false)));

        Dataset<Row> windowedDF = enrichedDF
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
                        sum("total_bid_amount").alias("total_bid_amount")
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

                        // Simplified SQL query to match the test table schema
                        String upsertSQL = String.format(
                                "INSERT INTO %s (campaign_id, event_type, window_start_time, event_count, total_bid_amount, updated_at) " +
                                        "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                                        "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE SET " +
                                        "event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
                                        "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
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
                                    statement.setLong(4, record.getLong(record.fieldIndex("event_count")));
                                    statement.setDouble(5, record.getDouble(record.fieldIndex("total_bid_amount")));
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