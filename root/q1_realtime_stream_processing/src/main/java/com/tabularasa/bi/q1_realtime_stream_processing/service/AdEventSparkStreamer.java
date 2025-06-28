package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.beans.factory.annotation.Qualifier;
import jakarta.annotation.PreDestroy;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Service for processing ad events using Spark Structured Streaming.
 * Implements best practices for Spark DataFrame operations and optimized processing.
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
    private final DataSource dataSource;
    private final ReentrantLock shutdownLock = new ReentrantLock();
    private final ThreadPoolTaskExecutor taskExecutor;
    private final JdbcTemplate jdbcTemplate;
    private final Map<String, String> kafkaOptions;

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

    @Value("${spark.streaming.checkpoint-location:/tmp/spark_checkpoints}")
    private String checkpointLocation;
    
    @Value("${spark.sql.shuffle.partitions:200}")
    private int shufflePartitions;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public AdEventSparkStreamer(
            SparkSession sparkSession, 
            MeterRegistry meterRegistry, 
            Tracer tracer, 
            JdbcTemplate jdbcTemplate,
            DataSource dataSource,
            @Qualifier("sparkStructuredStreamingKafkaOptions") Map<String, String> kafkaOptions) {
        this.sparkSession = sparkSession;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.jdbcTemplate = jdbcTemplate;
        this.dataSource = dataSource;
        this.kafkaOptions = kafkaOptions;
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
        
        // Enable Adaptive Query Execution
        sparkSession.conf().set("spark.sql.adaptive.enabled", "true");
        sparkSession.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");
        sparkSession.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");
        sparkSession.conf().set("spark.sql.adaptive.localShuffleReader.enabled", "true");
        sparkSession.conf().set("spark.sql.shuffle.partitions", shufflePartitions);
        
        // Check and fix database schema issues
        try {
            ensureDatabaseSchema();
        } catch (Exception e) {
            log.warn("Failed to fix database schema: {}", e.getMessage());
        }
    }
    
    /**
     * Ensure database schema by creating the table if it doesn't exist.
     * This is a temporary solution. A proper migration tool should be used.
     */
    private void ensureDatabaseSchema() {
        try {
            // Check if table exists and create it if needed
            Boolean tableExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'aggregated_campaign_stats')",
                Boolean.class);
                
            if (tableExists == null || !tableExists) {
                log.info("Table 'aggregated_campaign_stats' not found. Creating it.");
                jdbcTemplate.execute(
                    "CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (" +
                    "id SERIAL PRIMARY KEY, " +
                    "campaign_id VARCHAR(255) NOT NULL, " +
                    "event_type VARCHAR(50) NOT NULL, " +
                    "window_start_time TIMESTAMP NOT NULL, " +
                    "window_end_time TIMESTAMP NOT NULL, " +
                    "event_count BIGINT NOT NULL, " +
                    "total_bid_amount DECIMAL(18, 6) NOT NULL, " +
                    "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                    "CONSTRAINT aggregated_campaign_stats_unique UNIQUE (campaign_id, event_type, window_start_time)" +
                    ")"
                );
                
                // Create indexes for better query performance
                jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_agg_campaign_id ON aggregated_campaign_stats(campaign_id)");
                jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_agg_window_start ON aggregated_campaign_stats(window_start_time)");
                log.info("Table and indexes created successfully");
            }
        } catch (Exception e) {
            log.error("Error fixing database schema", e);
            throw e;
        }
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
                
                // Safe copy of the options map to avoid concurrent modification
                Map<String, String> streamOptions = new HashMap<>();
                if (kafkaOptions != null) {
                    streamOptions.putAll(kafkaOptions);
                }
                
                // Set kafka bootstrap servers directly
                streamOptions.remove("kafka.bootstrap.servers");
                streamOptions.remove("bootstrap.servers");
                streamOptions.put("kafka.bootstrap.servers", kafkaBootstrapServers);
                streamOptions.put("subscribe", inputTopic);
                
                // Configure optimal partitioning based on data size
                // Aim for 100-200MB per partition for better performance
                streamOptions.put("fetchOffset.numRetries", "5");
                streamOptions.put("failOnDataLoss", "false");
                
                // Create stream from Kafka with optimized settings
                DataStreamReader streamReader = sparkSession.readStream()
                    .format("kafka")
                    .option("maxOffsetsPerTrigger", 10000); // Adjust based on your throughput needs
                
                // Apply all options
                for (Map.Entry<String, String> option : streamOptions.entrySet()) {
                    if (!option.getKey().equals("format")) {
                        streamReader = streamReader.option(option.getKey(), option.getValue());
                    }
                }
                
                // Load data
                Dataset<Row> kafkaStream = streamReader.load();
                
                // Extract JSON from Kafka and parse it
                Dataset<Row> jsonStream = kafkaStream
                    .selectExpr("CAST(value AS STRING) as json", "CAST(key AS STRING) as key", "topic", "partition", "offset", "timestamp")
                    .select(functions.from_json(functions.col("json"), schema).as("data"), 
                            functions.col("key"), functions.col("topic"), functions.col("partition"), 
                            functions.col("offset"), functions.col("timestamp").as("kafka_timestamp"))
                    .select("data.*", "key", "topic", "partition", "offset", "kafka_timestamp")
                    .filter(functions.col("campaign_id").isNotNull());
                
                // Cache the filtered stream for better performance in subsequent operations
                // Use MEMORY_AND_DISK storage level for efficient processing
                jsonStream = jsonStream.persist(StorageLevel.MEMORY_AND_DISK());
                
                // Aggregate data by campaign_id and event_type with optimized windowing
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
                    );
                
                // Let Adaptive Query Execution handle partition sizing
                // This is better than manually setting coalesce(1)

                // Write the aggregated data to the database
                this.streamingQuery = aggregatedData.writeStream()
                    .outputMode(OutputMode.Update())
                    .foreachBatch(this::processBatch)
                    .option("checkpointLocation", checkpointLocation)
                    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                    .start();
                
                log.info("Spark Structured Streaming started successfully.");
                
            } catch (Exception e) {
                log.error("Failed to start Spark Structured Streaming", e);
                isRunning.set(false);
                throw new TimeoutException("Failed to start Spark Structured Streaming: " + e.getMessage());
            }
        } else {
            log.warn("Stream is already running.");
        }
    }
    
    private void processBatch(Dataset<Row> batchDF, Long batchId) {
        if (batchDF.isEmpty()) {
            return;
        }
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            log.info("Processing batch ID: {}", batchId);
            
            // Use explain to understand the query plan
            if (log.isDebugEnabled()) {
                log.debug("Batch {} execution plan:", batchId);
                batchDF.explain(true);
            }
            
            // Cache the batch data for multiple operations
            Dataset<Row> cachedBatch = batchDF.persist(StorageLevel.MEMORY_AND_DISK());
            
            // Get batch size for metrics
            long rowCount = cachedBatch.count();
            log.info("Batch {} contains {} rows", batchId, rowCount);
            processedEventsCounter.increment(rowCount);
            
            if (rowCount == 0) {
                log.warn("Batch {} is empty after count, skipping", batchId);
                return;
            }
            
            // Process data using JDBC batch operations for better performance
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                
                String upsertSql = "INSERT INTO aggregated_campaign_stats " +
                        "(campaign_id, event_type, window_start_time, window_end_time, event_count, total_bid_amount, updated_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, NOW()) " +
                        "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE SET " +
                        "event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
                        "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
                        "updated_at = NOW()";
                
                // Use efficient batch processing with optimal batch size
                final int BATCH_SIZE = 100;
                
                try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
                    int count = 0;
                    
                    // Collect data efficiently
                    List<Row> rows = cachedBatch.collectAsList();
                    
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
                            statement.setBigDecimal(6, row.getDecimal(4)); // total_bid_amount
                            statement.addBatch();
                            count++;
                            
                            if (count % BATCH_SIZE == 0) {
                                statement.executeBatch();
                                log.debug("Executed batch of {} records", BATCH_SIZE);
                            }
                        } catch (Exception e) {
                            log.error("Error processing row: {}", row.toString(), e);
                            failedEventsCounter.increment();
                        }
                    }
                    
                    if (count % BATCH_SIZE != 0) {
                        statement.executeBatch();
                    }
                    
                    connection.commit();
                    log.info("Successfully committed {} records to database", count);
                } catch (SQLException e) {
                    log.error("Error executing batch", e);
                    failedEventsCounter.increment(rowCount);
                    try {
                        connection.rollback();
                        log.info("Transaction rolled back");
                    } catch (SQLException re) {
                        log.error("Error during rollback", re);
                    }
                }
            } catch (Exception e) {
                log.error("Error processing batch {}", batchId, e);
                failedEventsCounter.increment(rowCount);
            } finally {
                // Unpersist cached data to free up memory
                cachedBatch.unpersist();
            }
        } catch (Exception e) {
            log.error("Error collecting batch data {}", batchId, e);
            failedEventsCounter.increment();
        } finally {
            sample.stop(batchProcessingTimer);
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
    
    @PreDestroy
    public void stopStream() {
        shutdownLock.lock();
        try {
            if (isRunning.compareAndSet(true, false)) {
                log.info("Attempting to stop Spark Structured Streaming query...");
                if (streamingQuery != null && streamingQuery.isActive()) {
                    try {
                        streamingQuery.stop();
                        log.info("Spark Structured Streaming query stopped successfully.");
                    } catch (Exception e) {
                        log.error("Error while stopping Spark streaming query", e);
                    }
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