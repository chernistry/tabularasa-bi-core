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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSource;

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
        
        // Оптимизируем connection pool
        this.dataSource = new BasicDataSource();
        this.dataSource.setDriverClassName(POSTGRES_DRIVER);
        this.dataSource.setUrl(dbUrl);
        this.dataSource.setUsername(dbUser);
        this.dataSource.setPassword(dbPassword);
        this.dataSource.setInitialSize(2);      // Уменьшаем начальный размер для экономии ресурсов
        this.dataSource.setMaxTotal(10);        // Уменьшаем максимальный размер
        this.dataSource.setMaxIdle(5);
        this.dataSource.setMinIdle(2);
        this.dataSource.setMaxWaitMillis(10000); // Уменьшаем время ожидания
        this.dataSource.setValidationQuery("SELECT 1");
        this.dataSource.setTestOnBorrow(true);
        this.dataSource.setTestWhileIdle(true);
        this.dataSource.setTimeBetweenEvictionRunsMillis(60000);
        // Добавляем автоматическое восстановление соединений
        this.dataSource.setRemoveAbandonedOnBorrow(true);
        this.dataSource.setRemoveAbandonedTimeout(60);
        this.dataSource.setLogAbandoned(true);
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
                
                // Создаем стрим из Kafka с оптимизированными настройками
                Dataset<Row> kafkaStream = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("subscribe", inputTopic)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "false")
                    .option("maxOffsetsPerTrigger", "5000")     // Уменьшаем для лучшего контроля нагрузки
                    .option("kafka.fetch.message.max.bytes", "5242880") // Уменьшаем размер данных
                    .option("kafka.max.partition.fetch.bytes", "5242880")
                    .load();
                
                // Извлекаем JSON из Kafka и парсим его
                Dataset<Row> jsonStream = kafkaStream
                    .selectExpr("CAST(value AS STRING) as json")
                    .select(functions.from_json(functions.col("json"), schema).as("data"))
                    .select("data.*")
                    .filter(functions.col("campaign_id").isNotNull())
                    // Используем меньшее количество партиций для локального режима
                    .coalesce(Math.max(2, sparkSession.sparkContext().defaultParallelism() / 2));
                
                // Регистрируем временную таблицу для SQL-запросов
                jsonStream.createOrReplaceTempView("ad_events");
                
                // Агрегируем данные по campaign_id и event_type с checkpointing
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
                
                // Используем foreachBatch с оптимизированным пулом соединений
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

                        // Используем небольшое количество партиций для локальной обработки
                        Dataset<Row> optimizedDF = batchDF.coalesce(2);
                        
                        // Кэшируем данные в оперативной памяти
                        optimizedDF.persist();
                        
                        long totalEventsInBatch = 0;
                        try {
                             totalEventsInBatch = optimizedDF.selectExpr("sum(event_count)").first().getLong(0);
                             log.info("Batch {} contains {} events", batchId, totalEventsInBatch);
                        } catch (Exception e) {
                            log.warn("Could not calculate total events in batch, maybe it was empty.", e);
                        }
                        
                        processedEventsCounter.increment(totalEventsInBatch);
                        
                        // Используем общий пул соединений, а не создаем новое соединение для каждой партиции
                        try {
                            // Добавляем повторные попытки для устойчивости
                            int retries = 0;
                            boolean success = false;
                            Exception lastError = null;
                            
                            while (!success && retries < 3) {
                                try {
                                    processBatchWithConnectionPool(optimizedDF);
                                    success = true;
                                } catch (Exception e) {
                                    retries++;
                                    lastError = e;
                                    log.warn("Error processing batch (attempt {}/3): {}", retries, e.getMessage());
                                    if (retries < 3) {
                                        Thread.sleep(1000 * retries); // Экспоненциальное ожидание
                                    }
                                }
                            }
                            
                            if (!success) {
                                failedEventsCounter.increment(totalEventsInBatch);
                                log.error("Failed to process batch after 3 attempts", lastError);
                            }
                        } finally {
                            optimizedDF.unpersist();
                            sample.stop(batchProcessingTimer);
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
     * Обрабатывает пакет данных с использованием пула соединений для улучшения производительности.
     *
     * @param batchDF Набор данных для обработки
     */
    private void processBatchWithConnectionPool(Dataset<Row> batchDF) {
        // Уменьшаем количество партиций для эффективной обработки
        Dataset<Row> cachedDF = batchDF.coalesce(2).cache();
        
        // Готовим SQL запрос для вставки/обновления
        final String insertSql =
            "INSERT INTO aggregated_campaign_stats " +
            "(campaign_id, event_type, window_start_time, event_count, total_bid_amount, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
            "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE " +
            "SET event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
            "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
            "updated_at = CURRENT_TIMESTAMP";
        
        try {
            // Используем foreachPartition для пакетной обработки с пулом соединений
            cachedDF.foreachPartition(partition -> {
                // Используем соединение из пула для всей партиции
                try (Connection connection = dataSource.getConnection();
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
                        statement.setLong(4, row.getAs("event_count"));
                        statement.setBigDecimal(5, row.getAs("total_bid_amount"));
                        statement.addBatch();
                        batchSize++;
                        totalRows++;
                        
                        // Выполняем пакет запросов каждые 200 строк (уменьшено для снижения нагрузки)
                        if (batchSize >= 200) {
                            try {
                                statement.executeBatch();
                                connection.commit();
                            } catch (SQLException e) {
                                log.error("Error executing batch: {}", e.getMessage());
                                connection.rollback();
                                throw e; // Перебрасываем для обработки выше
                            }
                            statement.clearBatch();
                            batchSize = 0;
                        }
                    }
                    
                    // Выполняем оставшиеся операции в пакете
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
                    log.error("Failed to write partition to database", e);
                    failedEventsCounter.increment();
                    throw new RuntimeException("Database error while processing partition", e);
                }
            });
            
        } finally {
            // Освобождаем ресурсы кэша
            cachedDF.unpersist();
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
            
            // Закрываем пул соединений
            try {
                if (dataSource != null) {
                    dataSource.close();
                    log.info("Connection pool closed successfully");
                }
            } catch (SQLException e) {
                log.error("Error closing connection pool", e);
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