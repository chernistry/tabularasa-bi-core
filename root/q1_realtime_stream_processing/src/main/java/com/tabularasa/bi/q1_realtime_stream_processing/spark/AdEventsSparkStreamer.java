package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import com.tabularasa.bi.q1_realtime_stream_processing.db.AdEventDBSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * Spark Structured Streaming processor for ad events from file source (Mode 1).
 *
 * <p>Reads events from JSONL files, aggregates by window, and writes results to the database sink.
 *
 * Preconditions: SparkSession and AdEventDBSink must be valid and non-null.
 * Postconditions: Aggregated campaign stats are persisted via the sink.
 */
@Component
@Profile("spark")
public class AdEventsSparkStreamer implements CommandLineRunner {
    // ================== FIELDS & CONFIGURATION ==================
    private static final Logger logger = LoggerFactory.getLogger(AdEventsSparkStreamer.class);
    private final SparkSession sparkSession;
    private final AdEventDBSink adEventDBSink;
    private StreamingQuery streamingQuery;
    @Value("${spark.streaming.source.file.path:q1_realtime_stream_processing/data/sample_ad_events.jsonl}")
    private String sourceFilePath;
    @Value("${spark.streaming.watermark.delay:15 seconds}")
    private String watermarkDelay;
    @Value("${spark.streaming.window.duration:1 minute}")
    private String windowDuration;
    @Value("${spark.streaming.trigger.interval:30 seconds}")
    private String triggerInterval;

    /**
     * Constructs the Spark streamer for ad events from file source.
     *
     * @param sparkSession   Spark session
     * @param adEventDBSink  Sink for persisting aggregated stats
     */
    @Autowired
    public AdEventsSparkStreamer(SparkSession sparkSession, AdEventDBSink adEventDBSink) {
        this.sparkSession = sparkSession;
        this.adEventDBSink = adEventDBSink;
    }

    // ================== STREAM LIFECYCLE ==================
    /**
     * CommandLineRunner entry point for running the Spark job.
     *
     * @param args Command-line arguments
     */
    @Override
    public void run(String... args) {
        if (sparkSession == null) {
            logger.warn("SparkSession is not initialized - Spark stream processing will not start.");
            return;
        }
        try {
            startStreamProcessing();
            logger.info("Spark stream processing initiated. Waiting for termination...");
            // Optionally: streamingQuery.awaitTermination();
        } catch (Exception e) {
            logger.error("Error during Spark stream processing startup", e);
        }
    }

    /**
     * Starts the Spark Structured Streaming job from file source.
     *
     * @throws TimeoutException if the stream fails to start
     */
    private void startStreamProcessing() throws TimeoutException {
        logger.info("Starting Spark stream processing from file source: {}", sourceFilePath);
        // Define the schema for incoming events
        StructType eventSchema = new StructType()
                .add("timestamp", DataTypes.TimestampType, false)
                .add("campaign_id", DataTypes.StringType, false)
                .add("event_type", DataTypes.StringType, false)
                .add("user_id", DataTypes.StringType, false)
                .add("bid_amount_usd", DataTypes.DoubleType, false);
        // Read the stream from the JSONL file source
        Dataset<Row> events = sparkSession
                .readStream()
                .schema(eventSchema)
                .option("maxFilesPerTrigger", 1)
                .json(sourceFilePath);
        // Perform aggregations
        Dataset<Row> aggregatedEvents = events
                .withWatermark("timestamp", watermarkDelay)
                .groupBy(
                        window(col("timestamp"), windowDuration),
                        col("campaign_id"),
                        col("event_type")
                )
                .agg(
                        count("*").as("event_count"),
                        sum("bid_amount_usd").as("total_bid_amount")
                )
                .select(
                        col("window.start").as("window_start_time"),
                        col("campaign_id"),
                        col("event_type"),
                        col("event_count"),
                        col("total_bid_amount")
                );
        // Start the streaming query to write to the database
        try {
            streamingQuery = aggregatedEvents
                    .writeStream()
                    .outputMode("update")
                    .foreachBatch((batchDF, batchId) -> {
                        logger.info("Processing batch #{} with {} records", batchId, batchDF.count());
                        if (!batchDF.isEmpty()) {
                            adEventDBSink.saveAggregatedCampaignStats(batchDF);
                        }
                    })
                    .trigger(Trigger.ProcessingTime(triggerInterval))
                    .start();
            logger.info("Spark stream processing query started successfully. Name: {}", streamingQuery.name());
        } catch (TimeoutException e) {
            logger.error("Failed to start Spark streaming query due to timeout", e);
            throw new RuntimeException("Failed to start Spark streaming query due to timeout", e);
        } catch (Exception e) {
            logger.error("An unexpected error occurred while starting Spark streaming query", e);
            throw new RuntimeException("An unexpected error occurred while starting Spark streaming query", e);
        }
    }

    // ================== SHUTDOWN HOOK ==================
    /**
     * Stops the Spark Structured Streaming job gracefully.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Attempting to stop Spark stream processing query...");
        try {
            if (streamingQuery != null && streamingQuery.isActive()) {
                streamingQuery.stop();
                logger.info("Spark stream processing query stopped successfully.");
            } else {
                logger.info("Spark stream processing query was not active or already stopped.");
            }
        } catch (Exception e) {
            logger.error("Error while stopping Spark stream processing query", e);
        }
    }
} 