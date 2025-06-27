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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeoutException;

/**
 * Service for processing ad events using Spark Structured Streaming.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AdEventSparkStreamer {

    private final SparkSession spark;
    private final AdEventDBSink adEventDBSink;

    @Value("${app.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${app.kafka.topics.ad-events}")
    private String inputTopic;

    @Value("${spark.streaming.checkpoint-location}")
    private String checkpointLocation;

    private StreamingQuery query;

    @PostConstruct
    public void init() throws TimeoutException {
        startStream();
    }

    public void startStream() throws TimeoutException {
        log.info("Initializing Spark Stream from topic [{}] to PostgreSQL", inputTopic);

        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", inputTopic)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        Dataset<AdEvent> adEvents = lines.map((MapFunction<String, AdEvent>) value -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(value, AdEvent.class);
        }, Encoders.bean(AdEvent.class));

        Dataset<AdEvent> filteredEvents = adEvents.filter((FilterFunction<AdEvent>) adEvent -> adEvent != null && adEvent.getCampaignId() != null);

        query = filteredEvents.writeStream()
                .foreach(adEventDBSink)
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .option("checkpointLocation", checkpointLocation)
                .start();

        log.info("Spark streaming query started.");
    }

    @PreDestroy
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