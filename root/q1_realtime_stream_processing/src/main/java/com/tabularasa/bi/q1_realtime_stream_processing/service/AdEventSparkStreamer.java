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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Service for processing ad events using Spark Core API.
 */
@Service
@Slf4j
@Profile("spark")
@SuppressWarnings("unused")
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

    // Default to /tmp/spark_checkpoints if property is not provided
    @Value("${spark.streaming.checkpoint-location:/tmp/spark_checkpoints}")
    private String checkpointLocation;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final String POSTGRES_DRIVER = "org.postgresql.Driver";

    @Autowired
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
                log.info("Table created successfully");
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
                
                // Set kafka bootstrap servers directly to avoid format mismatch issues
                // Remove any existing bootstrap servers config to avoid duplicates
                streamOptions.remove("kafka.bootstrap.servers");
                streamOptions.remove("bootstrap.servers");
                
                // Use the correct format for Spark Structured Streaming
                streamOptions.put("kafka.bootstrap.servers", kafkaBootstrapServers);
                streamOptions.put("subscribe", inputTopic);
                
                // Create stream from Kafka with corrected settings
                DataStreamReader streamReader = sparkSession.readStream()
                    .format("kafka")
                    .option("minPartitions", "1") // Set minimal partitions to avoid excessive parallelism
                    .option("maxOffsetsPerTrigger", 5000); // Explicit limit to avoid memory issues
                
                // Apply all options directly to avoid string format issues
                for (Map.Entry<String, String> option : streamOptions.entrySet()) {
                    if (!option.getKey().equals("format")) { // Skip format as it's already set
                        streamReader = streamReader.option(option.getKey(), option.getValue());
                    }
                }
                
                // Load data
                Dataset<Row> kafkaStream = streamReader.load();
                
                // Extract JSON from Kafka and parse it with increased error tolerance
                Dataset<Row> jsonStream = kafkaStream
                    .selectExpr("CAST(value AS STRING) as json", "CAST(key AS STRING) as key", "topic", "partition", "offset", "timestamp")
                    .select(functions.from_json(functions.col("json"), schema).as("data"), 
                            functions.col("key"), functions.col("topic"), functions.col("partition"), 
                            functions.col("offset"), functions.col("timestamp").as("kafka_timestamp"))
                    .select("data.*", "key", "topic", "partition", "offset", "kafka_timestamp")
                    .filter(functions.col("campaign_id").isNotNull());
                
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
                    );

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
        batchDF.persist();
        log.info("Processing batch ID: {}", batchId);

        String upsertSql = "INSERT INTO aggregated_campaign_stats (campaign_id, event_type, window_start_time, window_end_time, event_count, total_bid_amount, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE SET " +
                "event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
                "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
                "updated_at = NOW()";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(upsertSql)) {

            batchDF.foreachPartition(partition -> {
                try (Connection partitionConnection = dataSource.getConnection();
                     PreparedStatement partitionStatement = partitionConnection.prepareStatement(upsertSql)) {
                    
                    partition.forEachRemaining(row -> {
                        try {
                            Row window = row.getStruct(0);
                            Timestamp windowStart = window.getTimestamp(0);
                            Timestamp windowEnd = window.getTimestamp(1);

                            partitionStatement.setString(1, row.getString(1));
                            partitionStatement.setString(2, row.getString(2));
                            partitionStatement.setTimestamp(3, windowStart);
                            partitionStatement.setTimestamp(4, windowEnd);
                            partitionStatement.setLong(5, row.getLong(3));
                            partitionStatement.setBigDecimal(6, row.getDecimal(4));
                            partitionStatement.addBatch();
                        } catch (Exception e) {
                            log.error("Error processing row: {}", row.toString(), e);
                        }
                    });
                    partitionStatement.executeBatch();
                } catch (SQLException e) {
                    log.error("Error processing partition", e);
                    throw new RuntimeException(e);
                }
            });

        } catch (Exception e) {
            log.error("Error processing batch {}", batchId, e);
        } finally {
            batchDF.unpersist();
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