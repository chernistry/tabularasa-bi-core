package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabularasa.bi.q1_realtime_stream_processing.db.AdEventDBSink;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator;

import java.util.concurrent.TimeoutException;

/**
 * Service for processing ad events using Spark Structured Streaming.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Profile("spark")
public class AdEventSparkStreamer {

    private final SparkSession spark;

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

    private StreamingQuery query;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public void startStream() throws TimeoutException {
        log.info("Initializing Spark Stream from topic [{}] to PostgreSQL", inputTopic);
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(checkpointLocation));
        } catch (java.io.IOException e) {
            log.warn("Could not create checkpoint directory: {}", checkpointLocation, e);
        }
        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", inputTopic)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
        Dataset<AdEvent> adEvents = lines.map(
            (MapFunction<String, AdEvent>) value -> {
                if (value == null) return null;
                try {
                    return JSON_MAPPER.readValue(value, AdEvent.class);
                } catch (Exception e) {
                    return null;
                }
            }, Encoders.kryo(AdEvent.class));
        Dataset<AdEvent> filteredEvents = adEvents.filter(
                (FilterFunction<AdEvent>) adEvent -> adEvent != null && adEvent.getCampaignId() != null);

        // Use foreachBatch to perform bulk JDBC writes on the driver side. This avoids serializing a custom
        // ForeachWriter to executors, eliminating java.lang.IllegalStateException: unread block data errors that
        // occur when Spark attempts to deserialize complex writer instances across JVM versions.
        query = filteredEvents.writeStream()
                .foreachBatch((batchDS, batchId) -> {
                    org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> dbReady = batchDS.toDF()
                            .selectExpr(
                                    "eventId as event_id",
                                    "adCreativeId as ad_creative_id",
                                    "userId as user_id",
                                    "eventType as event_type",
                                    "timestamp",
                                    "bidAmountUsd as bid_amount_usd");

                    dbReady.write()
                            .format("jdbc")
                            .option("url", dbUrl)
                            .option("driver", "org.postgresql.Driver")
                            .option("dbtable", "ad_events")
                            .option("user", dbUser)
                            .option("password", dbPassword)
                            .mode("append")
                            .save();
                })
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .option("checkpointLocation", checkpointLocation)
                .start();
        log.info("Spark streaming query started.");
    }

    public void stopStream() {
        log.info("Stopping Spark streaming query...");
        if (query != null) {
            try {
                query.stop();
            } catch (TimeoutException e) {
                log.error("Timeout while stopping Spark streaming query", e);
            }
            log.info("Spark streaming query stopped.");
        }
    }
} 