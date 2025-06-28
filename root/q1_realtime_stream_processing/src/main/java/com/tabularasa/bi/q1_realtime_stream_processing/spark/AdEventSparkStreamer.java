package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.api.java.function.ForeachPartitionFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.sql.Timestamp;

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
            .add("spend_usd", "double");

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

        // Set Hadoop user explicitly to avoid authentication errors
        String currentUser = System.getProperty("user.name", "spark");
        System.setProperty("HADOOP_USER_NAME", currentUser);
        LOGGER.info("Setting HADOOP_USER_NAME to: {}", currentUser);

        String kafkaBootstrapServers = args[0];
        String adEventsTopic = args[1];
        String dbUrl = args[2];
        String dbUsername = args[3];
        String dbPassword = args.length > 4 ? args[4] : ""; // Password can be empty for some setups

        LOGGER.info("Starting with parameters: kafka={}, topic={}, dbUrl={}, dbUser={}", 
                kafkaBootstrapServers, adEventsTopic, dbUrl, dbUsername);

        // Setting system properties to resolve serialization issues
        System.setProperty("scala.collection.immutable.List.throwExceptionOnDetach", "false");
        System.setProperty("scala.collection.immutable.Vector.throwExceptionOnDetach", "false");
        System.setProperty("scala.collection.Seq.throwExceptionOnDetach", "false");
        System.setProperty("sun.io.serialization.extendedDebugInfo", "true");
        
        SparkSession spark = SparkSession.builder()
                .appName("AdEventSparkStreamer")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.default.parallelism", "1")
                .config("spark.streaming.kafka.maxRatePerPartition", "100")
                .config("spark.ui.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", "com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator")
                .config("spark.kryoserializer.buffer.max", "512m")
                .config("spark.kryoserializer.buffer", "256k")
                .config("spark.kryo.registrationRequired", "false")
                .config("spark.kryo.unsafe", "true")
                .config("spark.serializer.objectStreamReset", "100")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.sql.codegen.wholeStage", "false")
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
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", "500")
                .option("minPartitions", "1")
                .option("maxPartitions", "1")
                .load();

        LOGGER.info("Kafka source initialized. Parsing JSON data...");
        
        kafkaDF.printSchema();

        Dataset<Row> eventsDF = kafkaDF
                .select(from_json(col("value").cast("string"), AD_EVENT_SCHEMA).alias("event_data"))
                .select("event_data.*")
                .withColumn("event_timestamp", col("timestamp").cast("timestamp"));

        LOGGER.info("Schema after JSON parsing:");
        eventsDF.printSchema();

        LOGGER.info("Starting debug query to check data presence...");
        StreamingQuery debugQuery = eventsDF
            .writeStream()
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .start();
        
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        debugQuery.stop();

        Dataset<Row> enrichedDF = eventsDF
                .withColumn("total_bid_amount", coalesce(col("spend_usd"), lit(0.0)))
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

        // Forcibly reduce the number of partitions to 1
        enrichedDF = enrichedDF.coalesce(1);

        Dataset<Row> windowedDF = enrichedDF
                .withWatermark("event_timestamp", "10 seconds")
                .groupBy(
                        window(col("event_timestamp"), "1 minute"),
                        col("campaign_id"),
                        col("event_type")
                )
                .agg(
                        count("*").alias("event_count"),
                        sum("total_bid_amount").alias("total_bid_amount")
                );
                
        // Forcibly reduce the number of result partitions to 1
        windowedDF = windowedDF.coalesce(1);

        LOGGER.info("Starting streaming query with windowing to PostgreSQL...");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbUsername);
        connectionProperties.put("password", dbPassword);
        
        final int MAX_RETRIES = 3;

        StreamingQuery streamingQuery = windowedDF
                .writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {
                    LOGGER.info("Processing batch: {}", batchId);
                    if (batchDF.isEmpty()) {
                        LOGGER.warn("Batch {} is empty, skipping", batchId);
                        return;
                    }
                    
                    try {
                        // Convert data to a local collection of Java objects
                        List<Row> rows = batchDF.collectAsList();
                        LOGGER.info("Batch {} contains {} rows", batchId, rows.size());
                        
                        if (rows.isEmpty()) {
                            LOGGER.warn("Batch {} is empty after collect, skipping", batchId);
                            return;
                        }
                        
                        // Process data directly via JDBC
                        AtomicInteger retryCount = new AtomicInteger(0);
                        boolean success = false;
                        
                        while (!success && retryCount.get() < MAX_RETRIES) {
                            try (Connection connection = DriverManager.getConnection(dbUrl, connectionProperties)) {
                                connection.setAutoCommit(false);
                                
                                String upsertSQL = String.format(
                                        "INSERT INTO %s (campaign_id, event_type, window_start_time, window_end_time, event_count, total_bid_amount, updated_at) " +
                                                "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                                                "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE SET " +
                                                "event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
                                                "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
                                                "window_end_time = EXCLUDED.window_end_time, " +
                                                "updated_at = CURRENT_TIMESTAMP",
                                        TARGET_TABLE
                                );
                                
                                try (PreparedStatement statement = connection.prepareStatement(upsertSQL)) {
                                    int count = 0;
                                    
                                    for (Row row : rows) {
                                        try {
                                            Row window = row.getStruct(0);
                                            Timestamp windowStart = window.getTimestamp(0);
                                            Timestamp windowEnd = window.getTimestamp(1);
                                            
                                            statement.setString(1, row.getString(1)); // campaign_id
                                            statement.setString(2, row.getString(2)); // event_type
                                            statement.setTimestamp(3, windowStart);   // window_start_time
                                            statement.setTimestamp(4, windowEnd);     // window_end_time
                                            statement.setLong(5, row.getLong(3));     // event_count
                                            statement.setDouble(6, row.getDouble(4)); // total_bid_amount
                                            statement.addBatch();
                                            count++;
                                            
                                            if (count % 100 == 0) {
                                                statement.executeBatch();
                                                LOGGER.debug("Executed batch of {} records", count);
                                            }
                                        } catch (Exception e) {
                                            LOGGER.error("Error processing row: {}", row.toString(), e);
                                        }
                                    }
                                    
                                    if (count % 100 != 0) {
                                        statement.executeBatch();
                                    }
                                    
                                    connection.commit();
                                    LOGGER.info("Successfully committed {} records to database", count);
                                    success = true;
                                } catch (SQLException e) {
                                    LOGGER.error("Error executing batch", e);
                                    try {
                                        connection.rollback();
                                        LOGGER.info("Transaction rolled back");
                                    } catch (SQLException re) {
                                        LOGGER.error("Error during rollback", re);
                                    }
                                    
                                    int currentRetry = retryCount.incrementAndGet();
                                    LOGGER.info("Retry attempt {} of {}", currentRetry, MAX_RETRIES);
                                    
                                    if (currentRetry >= MAX_RETRIES) {
                                        LOGGER.error("Max retries reached, giving up on batch");
                                        throw new RuntimeException("Failed to process batch after " + MAX_RETRIES + " attempts", e);
                                    }
                                    
                                    try {
                                        Thread.sleep(1000 * currentRetry);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            } catch (SQLException e) {
                                LOGGER.error("Error establishing database connection", e);
                                int currentRetry = retryCount.incrementAndGet();
                                
                                if (currentRetry >= MAX_RETRIES) {
                                    LOGGER.error("Max retries reached, giving up on batch");
                                    throw new RuntimeException("Failed to establish database connection after " + MAX_RETRIES + " attempts", e);
                                }
                                
                                try {
                                    Thread.sleep(2000 * currentRetry);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error collecting batch data {}", batchId, e);
                    }
                })
                .option("checkpointLocation", "/tmp/spark-checkpoints/q1_ad_stream")
                .start();

        LOGGER.info("Spark Streaming job started. Query ID: {}", streamingQuery.id());
        streamingQuery.awaitTermination();
    }
}