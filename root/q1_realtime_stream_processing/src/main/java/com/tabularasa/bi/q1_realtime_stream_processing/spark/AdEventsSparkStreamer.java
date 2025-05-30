package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import com.tabularasa.bi.q1_realtime_stream_processing.db.AdEventDBSink;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

/**
 * Компонент для потоковой обработки событий рекламы с использованием Apache Spark
 */
@Component
public class AdEventsSparkStreamer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(AdEventsSparkStreamer.class);
    
    private final SparkSession sparkSession;
    private final AdEventDBSink adEventDBSink;
    private StreamingQuery streamingQuery;
    
    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;
    
    @Value("${kafka.topic.ad-events}")
    private String kafkaTopic;
    
    @Value("${spark.streaming.trigger.interval:10 seconds}")
    private String triggerInterval;

    @Autowired
    public AdEventsSparkStreamer(SparkSession sparkSession, AdEventDBSink adEventDBSink) {
        this.sparkSession = sparkSession;
        this.adEventDBSink = adEventDBSink;
    }

    @Override
    public void run(String... args) {
        if (sparkSession == null) {
            logger.warn("SparkSession не инициализирована - поток Spark не будет запущен");
            return;
        }
        
        try {
            startStreamProcessing();
        } catch (Exception e) {
            logger.error("Ошибка при запуске потоковой обработки Spark", e);
        }
    }

    private void startStreamProcessing() {
        logger.info("Запуск потоковой обработки Spark");
        
        // Определение схемы входящих событий
        StructType eventSchema = new StructType()
            .add("timestamp", DataTypes.TimestampType, false)
            .add("campaign_id", DataTypes.LongType, false)
            .add("user_id", DataTypes.StringType, false)
            .add("event_type", DataTypes.StringType, false)
            .add("bid_amount_usd", DataTypes.DoubleType, false);
            
        try {
            // Чтение потока из Kafka
            Dataset<Row> events = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false)
                .load()
                .selectExpr("CAST(value AS STRING) as json_data")
                .select(from_json(col("json_data"), eventSchema).as("data"))
                .select("data.*");
                
            // Добавление столбцов с временным окном
            Dataset<Row> windowedEvents = events
                .withWatermark("timestamp", "1 minute")
                .groupBy(
                    window(col("timestamp"), "30 seconds"),
                    col("campaign_id")
                )
                .agg(
                    sum(when(col("event_type").equalTo("impression"), 1).otherwise(0)).as("impressions"),
                    sum(when(col("event_type").equalTo("click"), 1).otherwise(0)).as("clicks"),
                    sum(when(col("event_type").equalTo("conversion"), 1).otherwise(0)).as("conversions"),
                    sum("bid_amount_usd").as("total_bid_amount_usd")
                )
                .withColumn("window_start", col("window.start"))
                .withColumn("window_end", col("window.end"))
                .drop("window");
                
            // Запуск стриминговой обработки с сохранением в БД
            streamingQuery = windowedEvents
                .writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {
                    logger.debug("Обработка пакета #{} с {} записями", batchId, batchDF.count());
                    if (!batchDF.isEmpty()) {
                        batchDF.persist();
                        adEventDBSink.saveAggregatedCampaignStats(batchDF);
                        batchDF.unpersist();
                    }
                })
                .trigger(Trigger.ProcessingTime(triggerInterval))
                .start();
                
            logger.info("Потоковая обработка запущена успешно");
            
        } catch (Exception e) {
            logger.error("Ошибка при настройке потоковой обработки", e);
            throw new RuntimeException("Ошибка при настройке потоковой обработки", e);
        }
    }
    
    @PreDestroy
    public void shutdown() {
        logger.info("Остановка потоковой обработки Spark");
        try {
            if (streamingQuery != null && streamingQuery.isActive()) {
                streamingQuery.stop();
                logger.info("Потоковая обработка успешно остановлена");
            }
        } catch (Exception e) {
            logger.error("Ошибка при остановке потоковой обработки", e);
        }
    }
} 