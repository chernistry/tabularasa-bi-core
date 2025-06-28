package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.annotation.PreDestroy;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Service for processing ad events using Spark Core API.
 */
@Service
@Slf4j
@Profile("spark")
public class AdEventSparkStreamer {

    private final SparkSession sparkSession;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final Counter processedEventsCounter;
    private final Counter failedEventsCounter;
    private final Timer batchProcessingTimer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private StreamingQuery streamingQuery;
    private final BasicDataSource dataSource;
    private final ReentrantLock shutdownLock = new ReentrantLock();
    private final ThreadPoolTaskExecutor taskExecutor;

    @Value("${spring.datasource.url}")
    private String dbUrl;
    @Value("${spring.datasource.username}")
    private String dbUser;
    @Value("${spring.datasource.password}")
    private String dbPassword;

    @Value("${app.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${app.kafka.topics.ad-events}")
    private String inputTopic;

    // Default to /tmp/spark_checkpoints if property is not provided
    @Value("${spark.streaming.checkpoint-location:/tmp/spark_checkpoints}")
    private String checkpointLocation;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String POSTGRES_DRIVER = "org.postgresql.Driver";

    @Autowired
    public AdEventSparkStreamer(SparkSession sparkSession, MeterRegistry meterRegistry, Tracer tracer) {
        this.sparkSession = sparkSession;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.processedEventsCounter = meterRegistry.counter("app.events.processed", "type", "ad_event");
        this.failedEventsCounter = meterRegistry.counter("app.events.failed", "type", "ad_event");
        this.batchProcessingTimer = meterRegistry.timer("app.events.batch.processing.time", "type", "ad_event");
        
        // Create a thread pool for asynchronous operations
        this.taskExecutor = new ThreadPoolTaskExecutor();
        this.taskExecutor.setCorePoolSize(2);
        this.taskExecutor.setMaxPoolSize(5);
        this.taskExecutor.setQueueCapacity(100);
        this.taskExecutor.setThreadNamePrefix("spark-async-");
        this.taskExecutor.initialize();
        
        // Optimize connection pool
        this.dataSource = new BasicDataSource();
        this.dataSource.setDriverClassName(POSTGRES_DRIVER);
        this.dataSource.setUrl(dbUrl);
        this.dataSource.setUsername(dbUser);
        this.dataSource.setPassword(dbPassword);
        this.dataSource.setInitialSize(2);      // Reduce initial size to save resources
        this.dataSource.setMaxTotal(10);        // Reduce maximum size
        this.dataSource.setMaxIdle(5);
        this.dataSource.setMinIdle(2);
        this.dataSource.setMaxWaitMillis(10000); // Reduce wait time
        this.dataSource.setValidationQuery("SELECT 1");
        this.dataSource.setTestOnBorrow(true);
        this.dataSource.setTestWhileIdle(true);
        this.dataSource.setTimeBetweenEvictionRunsMillis(60000);
        // Add automatic connection recovery
        this.dataSource.setRemoveAbandonedOnBorrow(true);
        this.dataSource.setRemoveAbandonedTimeout(60);
        this.dataSource.setLogAbandoned(true);
    }

    public void startStream() throws TimeoutException {
        if (isRunning.compareAndSet(false, true)) {
            log.info("Initializing Spark Structured Streaming for topic [{}]", inputTopic);
            
            try {
                // Clear checkpoint directory to prevent issues with outdated offsets
                clearCheckpointDirectory();
                createCheckpointDirectory();
                
                // Define JSON schema for events
                StructType schema = new StructType()
                    .add("timestamp", DataTypes.TimestampType)
                    .add("campaign_id", DataTypes.StringType)
                    .add("event_id", DataTypes.StringType)
                    .add("ad_creative_id", DataTypes.StringType)
                    .add("event_type", DataTypes.StringType)
                    .add("user_id", DataTypes.StringType)
                    .add("bid_amount_usd", DataTypes.DoubleType);
                
                // Create stream from Kafka with corrected settings
                Dataset<Row> kafkaStream = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("subscribe", inputTopic)
                    .option("startingOffsets", "latest") // Changed from "earliest" to "latest"
                    .option("failOnDataLoss", "false")
                    .option("maxOffsetsPerTrigger", "5000")
                    // Use correct format for Kafka options
                    .option("kafka.fetch.message.max.bytes", "5242880")
                    .option("kafka.max.partition.fetch.bytes", "5242880")
                    .option("kafka.auto.offset.reset", "latest") // Add explicit setting for offset reset
                    .load();
                
                // Extract JSON from Kafka and parse it
                Dataset<Row> jsonStream = kafkaStream
                    .selectExpr("CAST(value AS STRING) as json")
                    .select(functions.from_json(functions.col("json"), schema).as("data"))
                    .select("data.*")
                    .filter(functions.col("campaign_id").isNotNull())
                    // Use fewer partitions for local mode
                    .coalesce(Math.max(2, sparkSession.sparkContext().defaultParallelism() / 2));
                
                // Register temporary table for SQL queries
                jsonStream.createOrReplaceTempView("ad_events");
                
                // Aggregate data by campaign_id and event_type with checkpointing
                Dataset<Row> aggregatedData = jsonStream
                    .withWatermark("timestamp", "1 minute")
                    .groupBy(
                        functions.window(functions.col("timestamp"), "1 minute"),
                        functions.col("campaign_id"),
                        functions.col("event_type")
                    )
                    .agg(
                        functions.count("*").as("event_count"),
                        functions.sum("bid_amount_usd").as("total_bid_amount")
                    )
                    .withColumn("processing_time", functions.current_timestamp());
                
                // Create serializable object with database connection parameters
                final DatabaseConfig dbConfig = new DatabaseConfig(dbUrl, dbUser, dbPassword);
                
                // Use foreachBatch with optimized connection pool
                this.streamingQuery = aggregatedData
                    .writeStream()
                    .outputMode(OutputMode.Update())
                    .option("checkpointLocation", checkpointLocation)
                    .trigger(Trigger.ProcessingTime("10 seconds"))
                    .foreachBatch((batchDF, batchId) -> {
                        Timer.Sample sample = Timer.start();
                        log.info("Processing batch ID: {}", batchId);
                        if (batchDF.isEmpty()) {
                            log.info("Batch {} is empty, skipping.", batchId);
                            return;
                        }

                        // Use a small number of partitions for local processing
                        Dataset<Row> optimizedDF = batchDF.coalesce(2);
                        
                        // Cache data in memory
                        optimizedDF.persist();
                        
                        long totalEventsInBatch = 0;
                        try {
                            // Use a safe approach to extract values
                            Row firstRow = null;
                            try {
                                firstRow = optimizedDF.selectExpr("sum(event_count)").first();
                            } catch (InterruptedException e) {
                                log.warn("Operation was interrupted while calculating batch size. Processing will continue with default metrics.", e);
                            }
                            
                            if (firstRow != null && !firstRow.isNullAt(0)) {
                                totalEventsInBatch = firstRow.getLong(0);
                                log.info("Batch {} contains {} events", batchId, totalEventsInBatch);
                                processedEventsCounter.increment(totalEventsInBatch);
                            } else {
                                log.info("Batch {} - could not determine event count, using default", batchId);
                                processedEventsCounter.increment(); // Increment counter by 1 by default
                            }
                        } catch (Exception e) {
                            log.warn("Could not calculate total events in batch. Will proceed with processing anyway.", e);
                        }
                        
                        // Use static method for data processing, avoiding capture of this
                        try {
                            // Add retries for resilience
                            int retries = 0;
                            boolean success = false;
                            Exception lastError = null;
                            
                            while (!success && retries < 3) {
                                try {
                                    // Call static method instead of instance method
                                    processBatchStatically(optimizedDF, dbConfig);
                                    success = true;
                                } catch (Exception e) {
                                    retries++;
                                    lastError = e;
                                    log.warn("Error processing batch (attempt {}/3): {}", retries, e.getMessage());
                                    if (retries < 3) {
                                        Thread.sleep(1000 * retries); // Exponential backoff
                                    }
                                }
                            }
                            
                            if (!success) {
                                failedEventsCounter.increment(totalEventsInBatch > 0 ? totalEventsInBatch : 1);
                                log.error("Failed to process batch after 3 attempts", lastError);
                            }
                        } catch (InterruptedException e) {
                            log.warn("Batch processing was interrupted", e);
                            Thread.currentThread().interrupt(); // Restore interrupt flag
                        } finally {
                            // Always release resources in finally block
                            try {
                                optimizedDF.unpersist();
                            } catch (Exception e) {
                                log.warn("Error unpersisting dataframe: {}", e.getMessage());
                            }
                            
                            try {
                                sample.stop(batchProcessingTimer);
                            } catch (Exception e) {
                                log.warn("Error stopping timer: {}", e.getMessage());
                            }
                        }
                    })
                    .start();
                
                log.info("Spark Structured Streaming started successfully");
                
            } catch (Exception e) {
                isRunning.set(false);
                failedEventsCounter.increment();
                log.error("Failed to start Spark Structured Streaming", e);
                throw new TimeoutException("Failed to start Spark Structured Streaming: " + e.getMessage());
            }
        } else {
            log.info("Spark Structured Streaming already running");
        }
    }
    
    /**
     * Static class for storing database connection parameters.
     * Must be serializable for transmission to Spark worker nodes.
     */
    private static class DatabaseConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String url;
        private final String username;
        private final String password;
        
        public DatabaseConfig(String url, String username, String password) {
            this.url = url;
            this.username = username;
            this.password = password;
        }
        
        public String getUrl() {
            return url;
        }
        
        public String getUsername() {
            return username;
        }
        
        public String getPassword() {
            return password;
        }
    }
    
    /**
     * Static method for processing data partitions.
     * Does not capture the class instance, so it can be serialized and sent to worker nodes.
     *
     * @param batchDF Dataset to process
     * @param dbConfig Database connection parameters
     */
    private static void processBatchStatically(Dataset<Row> batchDF, DatabaseConfig dbConfig) {
        // Reduce the number of partitions for efficient processing
        Dataset<Row> cachedDF = batchDF.coalesce(2).cache();
        
        // Prepare SQL query for insert/update
        final String insertSql =
            "INSERT INTO aggregated_campaign_stats " +
            "(campaign_id, event_type, window_start_time, window_end_time, event_count, total_bid_amount, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
            "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE " +
            "SET event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
            "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
            "updated_at = CURRENT_TIMESTAMP";
        
        try {
            // Use foreachPartition for batch processing
            cachedDF.foreachPartition(partition -> {
                // Create connection for each partition
                try (Connection connection = DriverManager.getConnection(
                        dbConfig.getUrl(), dbConfig.getUsername(), dbConfig.getPassword());
                     PreparedStatement statement = connection.prepareStatement(insertSql)) {
                    
                    connection.setAutoCommit(false);
                    int batchSize = 0;
                    int totalRows = 0;
                    
                    while (partition.hasNext()) {
                        Row row = partition.next();
                        Row window = row.getAs("window");
                        statement.setString(1, row.getAs("campaign_id"));
                        statement.setString(2, row.getAs("event_type"));
                        statement.setTimestamp(3, window.getAs("start"));
                        statement.setTimestamp(4, window.getAs("end"));
                        statement.setLong(5, row.getAs("event_count"));
                        
                        // Handle null values for total_bid_amount
                        Double bidAmount = row.getAs("total_bid_amount");
                        statement.setBigDecimal(6, bidAmount == null ? 
                                               java.math.BigDecimal.ZERO : 
                                               java.math.BigDecimal.valueOf(bidAmount));
                        
                        statement.addBatch();
                        batchSize++;
                        totalRows++;
                        
                        // Execute batch of queries every 100 rows
                        if (batchSize >= 100) {
                            try {
                                statement.executeBatch();
                                connection.commit();
                            } catch (SQLException e) {
                                log.error("Error executing batch: {}", e.getMessage());
                                connection.rollback();
                                throw e;
                            }
                            statement.clearBatch();
                            batchSize = 0;
                        }
                    }
                    
                    // Execute remaining operations in batch
                    if (batchSize > 0) {
                        try {
                            statement.executeBatch();
                            connection.commit();
                        } catch (SQLException e) {
                            log.error("Error executing final batch: {}", e.getMessage());
                            connection.rollback();
                            throw e;
                        }
                    }
                    
                    log.debug("Processed {} rows in partition", totalRows);
                } catch (SQLException e) {
                    log.error("Failed to write partition to database: {}", e.getMessage(), e);
                    throw new RuntimeException("Database error while processing partition", e);
                }
            });
            
        } finally {
            // Release cache resources
            try {
                cachedDF.unpersist();
            } catch (Exception e) {
                log.warn("Error unpersisting cached dataframe: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Clears the checkpoint directory to prevent issues with outdated offsets.
     */
    private void clearCheckpointDirectory() {
        Path checkpointPath = Paths.get(checkpointLocation);
        if (Files.exists(checkpointPath)) {
            try {
                log.info("Clearing checkpoint directory: {}", checkpointPath.toAbsolutePath());
                Files.walk(checkpointPath)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            log.warn("Could not delete checkpoint file: {}", path, e);
                        }
                    });
                log.info("Checkpoint directory cleared successfully");
            } catch (IOException e) {
                log.warn("Could not clear checkpoint directory: {}", checkpointPath, e);
            }
        }
    }
    
    /**
     * Processes a batch of data using a connection pool to improve performance.
     * @deprecated Use the static method processBatchStatically instead of this method
     * @param batchDF Dataset to process
     */
    @Deprecated
    private void processBatchWithConnectionPool(Dataset<Row> batchDF) {
        // Method retained for backward compatibility, but should not be used
        throw new UnsupportedOperationException("This method is deprecated. Use processBatchStatically instead.");
    }
    
    @PreDestroy
    public void stopStream() {
        shutdownLock.lock();
        try {
            if (isRunning.compareAndSet(true, false)) {
                log.info("Stopping Spark Structured Streaming");
                if (streamingQuery != null && streamingQuery.isActive()) {
                    try {
                        streamingQuery.stop();
                        log.info("Streaming query stopped successfully");
                    } catch (Exception e) {
                        log.error("Error stopping streaming query", e);
                    }
                }
                
                // Close connection pool
                try {
                    if (dataSource != null) {
                        dataSource.close();
                        log.info("Connection pool closed successfully");
                    }
                } catch (SQLException e) {
                    log.error("Error closing connection pool", e);
                }
                
                // Stop thread pool
                try {
                    if (taskExecutor != null) {
                        taskExecutor.shutdown();
                        log.info("Task executor shutdown initiated");
                    }
                } catch (Exception e) {
                    log.error("Error shutting down task executor", e);
                }
            }
        } finally {
            shutdownLock.unlock();
        }
    }
    
    private void createCheckpointDirectory() {
        try {
            Path checkpointPath = Paths.get(checkpointLocation);
            Files.createDirectories(checkpointPath);
            log.info("Created checkpoint directory: {}", checkpointPath.toAbsolutePath());
        } catch (IOException e) {
            log.warn("Could not create checkpoint directory: {}", checkpointLocation, e);
            // Try to create a fallback directory in /tmp
            try {
                String fallbackPath = "/tmp/spark_checkpoints_" + System.currentTimeMillis();
                Files.createDirectories(Paths.get(fallbackPath));
                checkpointLocation = fallbackPath;
                log.info("Created fallback checkpoint directory: {}", fallbackPath);
            } catch (IOException ex) {
                log.error("Failed to create fallback checkpoint directory", ex);
                throw new RuntimeException("Could not create checkpoint directory", ex);
            }
        }
    }
} 