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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for processing ad events using Spark Core API.
 */
@Service
@Slf4j
@Profile("spark")
public class AdEventSparkStreamer {

    private final JavaSparkContext sparkContext;
    private final SparkSession sparkSession;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final Counter processedEventsCounter;
    private final Counter failedEventsCounter;
    private final Timer batchProcessingTimer;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private StreamingQuery streamingQuery;

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
    public AdEventSparkStreamer(JavaSparkContext sparkContext, SparkSession sparkSession, MeterRegistry meterRegistry, Tracer tracer) {
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.processedEventsCounter = meterRegistry.counter("app.events.processed", "type", "ad_event");
        this.failedEventsCounter = meterRegistry.counter("app.events.failed", "type", "ad_event");
        this.batchProcessingTimer = meterRegistry.timer("app.events.batch.processing.time", "type", "ad_event");
    }

    public void startStream() throws TimeoutException {
        if (isRunning.compareAndSet(false, true)) {
            log.info("Initializing Spark Structured Streaming for topic [{}]", inputTopic);
            
            try {
                createCheckpointDirectory();
                
                // Определяем схему JSON для событий
                StructType schema = new StructType()
                    .add("timestamp", DataTypes.TimestampType)
                    .add("campaign_id", DataTypes.StringType)
                    .add("event_id", DataTypes.StringType)
                    .add("ad_creative_id", DataTypes.StringType)
                    .add("event_type", DataTypes.StringType)
                    .add("user_id", DataTypes.StringType)
                    .add("bid_amount_usd", DataTypes.DoubleType);
                
                // Создаем стрим из Kafka
                Dataset<Row> kafkaStream = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("subscribe", inputTopic)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .load();
                
                // Извлекаем JSON из Kafka и парсим его
                Dataset<Row> jsonStream = kafkaStream
                    .selectExpr("CAST(value AS STRING) as json")
                    .select(functions.from_json(functions.col("json"), schema).as("data"))
                    .select("data.*");
                
                // Регистрируем временную таблицу для SQL-запросов
                jsonStream.createOrReplaceTempView("ad_events");
                
                // Агрегируем данные по campaign_id и event_type
                Dataset<Row> aggregatedData = sparkSession.sql(
                    "SELECT campaign_id, event_type, " +
                    "window(timestamp, '1 minute') as window, " +
                    "count(*) as event_count, " +
                    "sum(bid_amount_usd) as total_bid_amount " +
                    "FROM ad_events " +
                    "GROUP BY campaign_id, event_type, window"
                );
                
                // Функция для записи каждого батча в PostgreSQL
                ForeachWriter<Row> postgresWriter = new ForeachWriter<Row>() {
                    private Connection connection;
                    private PreparedStatement statement;
                    private final String insertSql = 
                        "INSERT INTO aggregated_campaign_stats " +
                        "(campaign_id, event_type, window_start_time, event_count, total_bid_amount, updated_at) " +
                        "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                        "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE " +
                        "SET event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
                        "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
                        "updated_at = CURRENT_TIMESTAMP";
                    
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        try {
                            Class.forName(POSTGRES_DRIVER);
                            connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
                            statement = connection.prepareStatement(insertSql);
                            return true;
                        } catch (Exception e) {
                            log.error("Error opening database connection", e);
                            return false;
                        }
                    }
                    
                    @Override
                    public void process(Row row) {
                        try {
                            // Извлекаем данные из Row
                            String campaignId = row.getAs("campaign_id");
                            String eventType = row.getAs("event_type");
                            Row window = row.getAs("window");
                            java.sql.Timestamp windowStart = window.getAs("start");
                            Long eventCount = row.getAs("event_count");
                            Double totalBidAmount = row.getAs("total_bid_amount");
                            
                            // Заполняем PreparedStatement
                            statement.setString(1, campaignId);
                            statement.setString(2, eventType);
                            statement.setTimestamp(3, windowStart);
                            statement.setLong(4, eventCount);
                            statement.setBigDecimal(5, new java.math.BigDecimal(totalBidAmount));
                            
                            // Выполняем запрос
                            statement.executeUpdate();
                            
                            // Увеличиваем счетчик обработанных событий
                            processedEventsCounter.increment(eventCount);
                            
                        } catch (SQLException e) {
                            log.error("Error writing to database", e);
                            failedEventsCounter.increment();
                        }
                    }
                    
                    @Override
                    public void close(Throwable errorOrNull) {
                        try {
                            if (statement != null) statement.close();
                            if (connection != null) connection.close();
                        } catch (SQLException e) {
                            log.error("Error closing database connection", e);
                        }
                    }
                };
                
                // Запускаем стрим и записываем результаты в PostgreSQL
                this.streamingQuery = aggregatedData
                    .writeStream()
                    .outputMode(OutputMode.Update())
                    .foreach(postgresWriter)
                    .option("checkpointLocation", checkpointLocation)
                    .trigger(Trigger.ProcessingTime("10 seconds"))
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
    
    @PreDestroy
    public void stopStream() {
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