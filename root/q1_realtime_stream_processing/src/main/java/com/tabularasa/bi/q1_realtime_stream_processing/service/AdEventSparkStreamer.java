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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.storage.StorageLevel;

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

    private static final String TARGET_TABLE = "aggregated_campaign_stats";

    // Extended schema with additional fields that may be present in real data
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
        
        // Enable Adaptive Query Execution
        sparkSession.conf().set("spark.sql.adaptive.enabled", "true");
        sparkSession.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");
        sparkSession.conf().set("spark.sql.adaptive.skewJoin.enabled", "true");
        sparkSession.conf().set("spark.sql.adaptive.localShuffleReader.enabled", "true");

        // Validate shuffle partitions value to prevent Spark runtime errors
        int effectiveShufflePartitions = shufflePartitions > 0
                ? shufflePartitions
                : Math.max(1, sparkSession.sparkContext().defaultParallelism());

        if (shufflePartitions <= 0) {
            log.warn("Invalid spark.sql.shuffle.partitions value ({}). Falling back to {}.", shufflePartitions, effectiveShufflePartitions);
        }

        sparkSession.conf().set("spark.sql.shuffle.partitions", effectiveShufflePartitions);
    }

    public void startStream() throws TimeoutException {
        if (isRunning.compareAndSet(false, true)) {
            log.info("Initializing Spark Structured Streaming for topic [{}]", inputTopic);
            
            // TODO: In a production environment, clearing the checkpoint directory should be avoided
            // as it can lead to data loss or reprocessing. This is here for development convenience.
            // Consider a strategy for managing checkpoints, such as versioning or manual cleanup when needed.
            createCheckpointDirectory();
            
            Dataset<Row> kafkaStream = sparkSession.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("subscribe", inputTopic)
                    .option("failOnDataLoss", "false") // Best practice for production to not lose data
                    .option("startingOffsets", "earliest") // Process all data from the beginning on first start
                    .option("maxOffsetsPerTrigger", 10000) // Rate limiting
                    .load();

            Dataset<Row> eventsDF = kafkaStream
                    .select(from_json(col("value").cast("string"), AD_EVENT_SCHEMA).alias("event_data"))
                    .select("event_data.*")
                    .withColumn("event_timestamp", col("timestamp").cast("timestamp"));

            Dataset<Row> enrichedDF = eventsDF
                    .withColumn("total_bid_amount", coalesce(col("spend_usd"), lit(0.0)))
                    .na().fill("unknown", new String[]{"device_type", "country_code", "product_brand", "product_age_group"})
                    .na().fill(0, new String[]{"product_category_1", "product_category_2", "product_category_3", "product_category_4", "product_category_5", "product_category_6", "product_category_7"})
                    .na().fill(0.0, new String[]{"sales_amount_euro"})
                    .na().fill(false, new String[]{"sale"});

            Dataset<Row> aggregatedData = enrichedDF
                    .withWatermark("event_timestamp", "1 minute")
                    .groupBy(
                            window(col("event_timestamp"), "1 minute"),
                            col("campaign_id"),
                            col("event_type")
                    )
                    .agg(
                            count("*").as("event_count"),
                            sum("total_bid_amount").as("total_bid_amount")
                    );

            this.streamingQuery = aggregatedData.writeStream()
                    .outputMode(OutputMode.Update())
                    .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                    .option("checkpointLocation", checkpointLocation)
                    .foreachBatch(this::processBatch)
                    .start();

            log.info("Spark Streaming job started. Query ID: {}", streamingQuery.id());
        }
    }
    
    private void processBatch(Dataset<Row> batchDF, Long batchId) {
        final int MAX_RETRIES = 3;
        int retry = 0;
        boolean success = false;

        Timer.Sample sample = Timer.start(meterRegistry);

        log.info("Processing batch: {}", batchId);
        if (batchDF.isEmpty()) {
            log.warn("Batch {} is empty, skipping.", batchId);
            return;
        }

        // Cache the DataFrame to avoid recomputation
        batchDF.persist(StorageLevel.MEMORY_AND_DISK());

        try {
            List<Row> rows = batchDF.collectAsList();
            log.info("Batch {} contains {} rows to be written.", batchId, rows.size());

            if (rows.isEmpty()) {
                log.warn("Batch {} is empty after collect, skipping.", batchId);
                return;
            }

            while (!success && retry < MAX_RETRIES) {
                try (Connection connection = dataSource.getConnection()) {
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
                        int batchSize = 0;
                        for (Row row : rows) {
                            try {
                                Row window = row.getStruct(0);
                                statement.setString(1, row.getString(1)); // campaign_id
                                statement.setString(2, row.getString(2)); // event_type
                                statement.setTimestamp(3, window.getTimestamp(0));   // window_start_time
                                statement.setTimestamp(4, window.getTimestamp(1));     // window_end_time
                                statement.setLong(5, row.getLong(3));     // event_count
                                statement.setDouble(6, row.getDouble(4)); // total_bid_amount
                                statement.addBatch();
                                batchSize++;
                            } catch (Exception e) {
                                log.error("Error processing row, skipping: {}", row.toString(), e);
                                failedEventsCounter.increment();
                            }
                        }
                        statement.executeBatch();
                        connection.commit();
                        log.info("Successfully committed {} records to database for batch {}", batchSize, batchId);
                        processedEventsCounter.increment(batchSize);
                        success = true; // Mark as success to exit the while loop
                    } catch (SQLException e) {
                        log.error("Error executing batch for batchId: {}, attempt {}/{}", batchId, retry + 1, MAX_RETRIES, e);
                        connection.rollback();
                        retry++;
                        Thread.sleep(2000L * retry);
                    }
                } catch (SQLException | InterruptedException e) {
                    log.error("Error with DB connection for batchId: {}, attempt {}/{}", batchId, retry + 1, MAX_RETRIES, e);
                    retry++;
                    try {
                        Thread.sleep(2000L * retry);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } // end while

            if (!success) {
                log.error("Failed to process batch {} after {} retries. Giving up.", batchId, MAX_RETRIES);
                failedEventsCounter.increment(rows.size());
            }

        } finally {
            batchDF.unpersist();
            sample.stop(batchProcessingTimer);
        }
    }
    
    @PreDestroy
    public void stopStream() {
        shutdownLock.lock();
        try {
            if (isRunning.compareAndSet(true, false)) {
                log.info("Attempting to gracefully stop Spark Structured Streaming query...");
                if (streamingQuery != null && streamingQuery.isActive()) {
                    try {
                        streamingQuery.stop();
                        log.info("Spark Streaming query stopped successfully.");
                    } catch (TimeoutException e) {
                        log.error("Timeout while stopping Spark Streaming query", e);
                    }
                }
                if (taskExecutor != null) {
                    taskExecutor.shutdown();
                }
                log.info("Spark resources released.");
            }
        } finally {
            shutdownLock.unlock();
        }
    }
    
    private void createCheckpointDirectory() {
        try {
            Path checkpointPath = Paths.get(checkpointLocation);
            if (!Files.exists(checkpointPath)) {
                Files.createDirectories(checkpointPath);
                log.info("Created checkpoint directory: {}", checkpointLocation);
            }
        } catch (IOException e) {
            log.error("Failed to create checkpoint directory: {}", checkpointLocation, e);
            throw new RuntimeException("Failed to create checkpoint directory", e);
        }
    }
} 