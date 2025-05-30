package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import com.tabularasa.bi.q1_realtime_stream_processing.kafka.producer.AdEventProducer; // Keep if needed for producing from Spark, otherwise remove
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException; // Keep if specific exception handling is planned
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Properties; // Added for JDBC
import java.sql.Connection; // Added for JDBC
import java.sql.DriverManager; // Added for JDBC
import java.sql.PreparedStatement; // Added for JDBC
import java.sql.SQLException; // Added for JDBC

// Static imports for Spark SQL functions
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.functions; // For other functions like count, sum, etc.

@Component
public class AdEventSparkStreamer {

    private static final Logger logger = LoggerFactory.getLogger(AdEventSparkStreamer.class);

    private final SparkSession sparkSession;
    private final Map<String, String> kafkaConsumerProperties;
    private final String adEventsTopic;
    // private final AdEventProducer adEventProducer; // If you want to produce data from Spark

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

    private static final String TARGET_TABLE = "ad_campaign_summary";
    
    private static final StructType AD_EVENT_SCHEMA = new StructType()
            .add("timestamp", "timestamp")
            .add("campaign_id", "string")
            .add("event_type", "string")
            .add("user_id", "string")
            .add("bid_amount_usd", "double");


    @Autowired
    public AdEventSparkStreamer(SparkSession sparkSession, 
                                @Qualifier("sparkKafkaConsumerProperties") Map<String, String> kafkaConsumerProperties,
                                @Value("${kafka.topic.ad_events:ad_events_topic}") String adEventsTopic) {
        this.sparkSession = sparkSession;
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.adEventsTopic = adEventsTopic;
    }

    @PostConstruct
    public void startStream() throws java.util.concurrent.TimeoutException {
        logger.info("Initializing Spark Streaming job for topic: {}", adEventsTopic);

        Dataset<Row> kafkaDF = sparkSession
                .readStream()
                .format("kafka")
                .option("subscribe", adEventsTopic)
                .option("failOnDataLoss", "false") 
                .options(kafkaConsumerProperties)
                .load();

        Dataset<Row> eventsDF = kafkaDF
                .select(from_json(col("value").cast("string"), AD_EVENT_SCHEMA).alias("event_data"))
                .select("event_data.*");

        Dataset<Row> aggregatedDF = eventsDF
                .groupBy("campaign_id", "event_type")
                .agg(
                        functions.count("*").alias("event_count"),
                        functions.sum("bid_amount_usd").alias("total_bid_amount")
                );
        
        logger.info("Starting streaming query to PostgreSQL (upsert aggregation)...");

        streamingQuery = aggregatedDF
                .writeStream()
                .outputMode("complete")
                .foreachBatch((batchDF, batchId) -> {
                    logger.info("Processing batch: " + batchId);
                    batchDF.persist(); // Persist to avoid recomputation if multiple actions are performed

                    batchDF.foreachPartition(partitionOfRecords -> {
                        // Connection properties
                        Properties connectionProperties = new Properties();
                        connectionProperties.put("user", dbUsername);
                        connectionProperties.put("password", dbPassword);
                        // Ensure the PostgreSQL JDBC driver is registered
                        Class.forName("org.postgresql.Driver");
                        
                        Connection connection = null;
                        PreparedStatement statement = null;
                        
                        String upsertSQL = String.format(
                            "INSERT INTO %s (campaign_id, event_type, event_count, total_bid_amount, last_updated) " +
                            "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                            "ON CONFLICT (campaign_id, event_type) DO UPDATE SET " +
                            "event_count = EXCLUDED.event_count, " +
                            "total_bid_amount = EXCLUDED.total_bid_amount, " +
                            "last_updated = CURRENT_TIMESTAMP",
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
                                statement.setLong(3, record.getLong(record.fieldIndex("event_count")));
                                statement.setDouble(4, record.getDouble(record.fieldIndex("total_bid_amount")));
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
                    batchDF.unpersist(); // Unpersist after processing
                })
                .start();

        logger.info("Spark Streaming job started. Query ID: {}", streamingQuery.id());
    }

    @PreDestroy
    public void stopStream() {
        logger.info("Stopping Spark Streaming job...");
        if (streamingQuery != null && streamingQuery.isActive()) {
            try {
                streamingQuery.stop();
                logger.info("Spark Streaming job stopped successfully.");
            } catch (java.util.concurrent.TimeoutException e) {
                logger.error("Timeout while stopping Spark Streaming job", e);
            } catch (Throwable t) {
                 logger.error("Serious error/exception stopping Spark Streaming job", t);
            }
        }
        if (sparkSession != null) {
            sparkSession.stop();
            logger.info("SparkSession stopped.");
        }
    }
} 