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

    public void startStream() throws TimeoutException {
        log.info("Initializing Spark Stream from topic [{}] to PostgreSQL", inputTopic);
        spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.conf().set("spark.kryo.registrator", KryoRegistrator.class.getName());
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
                    return new com.fasterxml.jackson.databind.ObjectMapper().readValue(value, AdEvent.class);
                } catch (Exception e) {
                    return null;
                }
            }, Encoders.kryo(AdEvent.class));
        Dataset<AdEvent> filteredEvents = adEvents.filter((FilterFunction<AdEvent>) adEvent -> adEvent != null && adEvent.getCampaignId() != null);
        query = filteredEvents.writeStream()
                .foreach(new AdEventDBSink(dbUrl, dbUser, dbPassword))
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