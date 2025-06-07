package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Properties; // Added for JDBC
import java.sql.Connection; // Added for JDBC
import java.sql.DriverManager; // Added for JDBC
import java.sql.PreparedStatement; // Added for JDBC
import java.sql.SQLException; // Added for JDBC

// Static imports for Spark SQL functions
import static org.apache.spark.sql.functions.*;

/**
 * Spark Structured Streaming processor for ad events from Kafka with 1-minute tumbling windows.
 *
 * Reads events from Kafka, aggregates by window, and writes results to PostgreSQL.
 *
 * Preconditions: SparkSession and Kafka properties must be valid and non-null.
 * Postconditions: Aggregated campaign stats are upserted into the target DB table.
 */
@Profile("spark")
public class AdEventSparkStreamer implements CommandLineRunner {

    // ================== FIELDS & CONFIGURATION ==================
    private static final Logger logger = LoggerFactory.getLogger(AdEventSparkStreamer.class);
    private final SparkSession sparkSession;
    private final Map<String, String> kafkaConsumerProperties;
    private final String adEventsTopic;
    private StreamingQuery streamingQuery;
    @Value("${spark.app.name:Q1AdEventStreamingApp}")
    private String appName;
    @Value("${spark.master:local[*]}")
    private String sparkMaster;
    @Value("${spring.datasource.url}")
    private String dbUrl;
    @Value("${spring.datasource.username}")
    private String dbUsername;
    @Value("${spring.datasource.password}")
    private String dbPassword;
    private static final String TARGET_TABLE = "aggregated_campaign_stats";
    private static final StructType AD_EVENT_SCHEMA = new StructType()
            .add("timestamp", "timestamp")
            .add("campaign_id", "string")
            .add("event_type", "string")
            .add("user_id", "string")
            .add("bid_amount_usd", "double");

    /**
     * Constructs the Spark streamer for ad events.
     *
     * @param sparkSession Spark session
     * @param kafkaConsumerProperties Kafka consumer properties
     * @param adEventsTopic Kafka topic for ad events
     */
    @Autowired
    public AdEventSparkStreamer(SparkSession sparkSession,
                                @Qualifier("sparkKafkaConsumerProperties") Map<String, String> kafkaConsumerProperties,
                                @Value("${kafka.topic.ad-events:ad_events_topic}") String adEventsTopic) {
        this.sparkSession = sparkSession;
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.adEventsTopic = adEventsTopic;
    }

    // ================== STREAM LIFECYCLE ==================
    /**
     * Starts the Spark Structured Streaming job after bean construction.
     *
     * @throws java.util.concurrent.TimeoutException if the stream fails to start
     */
    @PostConstruct
    public void startStream() throws java.util.concurrent.TimeoutException {
        logger.info("Initializing Spark Structured Streaming job for topic: {}", adEventsTopic);
        Dataset<Row> kafkaDF = sparkSession
                .readStream()
                .format("kafka")
                .option("subscribe", adEventsTopic)
                .option("failOnDataLoss", "false")
                .options(kafkaConsumerProperties)
                .load();
        // Parse JSON and cast timestamp field explicitly
        Dataset<Row> eventsDF = kafkaDF
                .select(from_json(col("value").cast("string"), AD_EVENT_SCHEMA).alias("event_data"))
                .select("event_data.*")
                .withColumn("event_timestamp", col("timestamp").cast("timestamp"));
        // Apply watermarking and tumbling window of 1 minute
        Dataset<Row> windowedDF = eventsDF
                .withWatermark("event_timestamp", "10 seconds")
                .groupBy(
                        window(col("event_timestamp"), "1 minute"),
                        col("campaign_id"),
                        col("event_type")
                )
                .agg(
                        count("*").alias("event_count"),
                        sum("bid_amount_usd").alias("total_bid_amount")
                );
        logger.info("Starting streaming query with windowing to PostgreSQL...");
        // Use update output mode for incremental processing
        streamingQuery = windowedDF
                .writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {
                    logger.info("Processing batch: " + batchId);
                    batchDF.persist(); // Persist to avoid recomputation if multiple actions are performed
                    // Extract window.start and window.end explicitly to prepare for PostgreSQL
                    Dataset<Row> processedDF = batchDF
                            .withColumn("window_start_time", col("window.start"))
                            .withColumn("window_end_time", col("window.end"))
                            .drop("window");
                    // For debugging - show schema and some data when processing
                    if (logger.isDebugEnabled()) {
                        logger.debug("Batch Schema:");
                        processedDF.printSchema();
                        if (!processedDF.isEmpty()) {
                            logger.debug("Sample data:");
                            processedDF.show(5, false);
                        }
                    }
                    processedDF.foreachPartition(partitionOfRecords -> {
                        // Connection properties
                        Properties connectionProperties = new Properties();
                        connectionProperties.put("user", dbUsername);
                        connectionProperties.put("password", dbPassword);
                        // Ensure the PostgreSQL JDBC driver is registered
                        Class.forName("org.postgresql.Driver");
                        Connection connection = null;
                        PreparedStatement statement = null;
                        String upsertSQL = String.format(
                                "INSERT INTO %s (campaign_id, event_type, window_start_time, event_count, total_bid_amount, updated_at) " +
                                        "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                                        "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE SET " +
                                        "event_count = EXCLUDED.event_count, " +
                                        "total_bid_amount = EXCLUDED.total_bid_amount, " +
                                        "updated_at = CURRENT_TIMESTAMP",
                                TARGET_TABLE
                        );
                        try {
                            connection = DriverManager.getConnection(dbUrl, connectionProperties);
                            connection.setAutoCommit(false); // For batching
                            statement = connection.prepareStatement(upsertSQL);
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
                                if (count % 1000 == 0) { // Commit every 1000 records
                                    statement.executeBatch();
                                    connection.commit();
                                    logger.info("Committed {} records in partition.", count);
                                }
                            }
                            statement.executeBatch(); // Execute remaining batch
                            connection.commit();
                            logger.info("Finished processing partition, committed {} records.", count);
                        } catch (Exception e) {
                            logger.error("Error processing partition or writing to DB", e);
                            if (connection != null) {
                                try {
                                    connection.rollback();
                                } catch (SQLException se) {
                                    logger.error("Rollback failed", se);
                                }
                            }
                            // Consider how to handle retries or dead-letter queue for failed batches
                            throw new RuntimeException("Error in foreachPartition", e);
                        } finally {
                            if (statement != null) {
                                try {
                                    statement.close();
                                } catch (SQLException se) {
                                    logger.warn("Error closing PreparedStatement", se);
                                }
                            }
                            if (connection != null) {
                                try {
                                    connection.close();
                                } catch (SQLException se) {
                                    logger.warn("Error closing Connection", se);
                                }
                            }
                        }
                    });
                    batchDF.unpersist();
                })
                .option("checkpointLocation", "/tmp/spark-checkpoints/q1_ad_stream")
                .start();

        logger.info("Spark Streaming job started. Query ID: {}", streamingQuery.id());
    }

    /**
     * Stops the Spark Structured Streaming job gracefully.
     */
    @PreDestroy
    public void stopStream() {
        logger.info("Stopping Spark Structured Streaming job...");
        if (streamingQuery != null && streamingQuery.isActive()) {
            try {
                streamingQuery.stop();
                logger.info("Streaming query stopped.");
            } catch (java.util.concurrent.TimeoutException e) {
                logger.error("Timeout while stopping Spark Streaming job", e);
            } catch (Throwable t) {
                logger.error("Serious error/exception stopping Spark Streaming job", t);
            }
        } else {
            logger.info("No active streaming query to stop.");
        }
        if (sparkSession != null) {
            sparkSession.stop();
            logger.info("SparkSession stopped.");
        }
    }

    /**
     * CommandLineRunner entry point for running the Spark job.
     *
     * @param args Command-line arguments
     * @throws Exception if the job fails to start
     */
    @Override
    public void run(String... args) throws Exception {
        startStream();
        if (streamingQuery != null) {
            streamingQuery.awaitTermination();
        }
    }
} 