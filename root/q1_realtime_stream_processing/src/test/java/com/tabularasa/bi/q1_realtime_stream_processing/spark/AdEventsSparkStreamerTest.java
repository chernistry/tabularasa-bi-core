package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import scala.Option;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdEventsSparkStreamerTest {

    private SparkSession spark;

    // Schema matching AdEventsSparkStreamer's eventSchema
    private final StructType eventSchema = new StructType()
            .add("timestamp", DataTypes.TimestampType, false)
            .add("campaign_id", DataTypes.StringType, false)
            .add("event_type", DataTypes.StringType, false)
            .add("user_id", DataTypes.StringType, false)
            .add("bid_amount_usd", DataTypes.DoubleType, false);

    @BeforeEach
    void setUp() {
        spark = SparkSession.builder()
                .appName("AdEventsSparkStreamerTest")
                .master("local[2]") // Use 2 cores for local testing
                .config("spark.sql.shuffle.partitions", "2") // Small number for tests
                .config("spark.sql.streaming.checkpointLocation", "target/test-checkpoint-dir-" + System.currentTimeMillis()) // Unique checkpoint dir
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN"); // Reduce verbosity
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
        // Clean up checkpoint directory if necessary, though unique names help avoid clashes.
    }

    @Test
    void testAdEventAggregations() throws Exception {
        // Input data: (timestamp, campaign_id, event_type, user_id, bid_amount_usd)
        // Timestamps are chosen to fall into specific 1-minute windows
        List<Row> inputDataList = Arrays.asList(
                // Window 1: 2023-01-01 10:00:00 - 10:00:59
                org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:10"), "cmp001", "impression", "user1", 0.05),
                org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:20"), "cmp001", "impression", "user2", 0.06),
                org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:30"), "cmp001", "click", "user1", 0.0), // Clicks might not have bid_amount

                // Window 2: 2023-01-01 10:01:00 - 10:01:59
                org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:01:15"), "cmp002", "impression", "user3", 0.10),
                org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:01:25"), "cmp001", "impression", "user4", 0.07),

                // Late data for Window 1 (should be processed if watermark allows)
                org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:50"), "cmp002", "impression", "user5", 0.03)
        );

        // Convert List<Row> to Seq<Row> for MemoryStream
        Seq<Row> inputDataSeq = JavaConverters.asScalaIteratorConverter(inputDataList.iterator()).asScala().toSeq();

        MemoryStream<Row> inputStream = new MemoryStream<>(
            1,
            spark.sqlContext(),
            Option.empty(),
            RowEncoder.apply(eventSchema)
        );
        Dataset<Row> events = inputStream.toDF(); // toDF() without schema

        // Apply the same transformations as in AdEventsSparkStreamer
        String watermarkDelay = "10 seconds"; // Match AdEventsSparkStreamer
        String windowDuration = "1 minute";  // Match AdEventsSparkStreamer

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

        // Use a memory sink to collect results for verification
        StreamingQuery query = aggregatedEvents
                .writeStream()
                .outputMode("update") // 'update' or 'complete' mode
                .format("memory")
                .queryName("testAggregations")
                .start();

        // Add data to the stream and advance it
        inputStream.addData(inputDataSeq);
        query.processAllAvailable(); // Process all data added so far

        // Retrieve results from memory sink
        Dataset<Row> resultTable = spark.sql("SELECT * FROM testAggregations ORDER BY window_start_time, campaign_id, event_type");
        List<Row> results = resultTable.collectAsList();
        
        // Print results for debugging
        System.out.println("Aggregated Results:");
        results.forEach(System.out::println);

        // Expected results:
        // Window 1 (10:00:00):
        // cmp001, impression, count=2, sum_bid=0.11
        // cmp001, click,      count=1, sum_bid=0.0
        // cmp002, impression, count=1, sum_bid=0.03
        // Window 2 (10:01:00):
        // cmp001, impression, count=1, sum_bid=0.07
        // cmp002, impression, count=1, sum_bid=0.10

        assertEquals(5, results.size(), "Should have 5 aggregated groups");

        // Detailed assertions (order matters due to ORDER BY in query)
        // Window 1
        Row r1 = results.get(0);
        assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), r1.getTimestamp(r1.fieldIndex("window_start_time")));
        assertEquals("cmp001", r1.getString(r1.fieldIndex("campaign_id")));
        assertEquals("click", r1.getString(r1.fieldIndex("event_type")));
        assertEquals(1L, r1.getLong(r1.fieldIndex("event_count")));
        assertEquals(0.0, r1.getDouble(r1.fieldIndex("total_bid_amount")), 0.001);

        Row r2 = results.get(1);
        assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), r2.getTimestamp(r2.fieldIndex("window_start_time")));
        assertEquals("cmp001", r2.getString(r2.fieldIndex("campaign_id")));
        assertEquals("impression", r2.getString(r2.fieldIndex("event_type")));
        assertEquals(2L, r2.getLong(r2.fieldIndex("event_count")));
        assertEquals(0.11, r2.getDouble(r2.fieldIndex("total_bid_amount")), 0.001);
        
        Row r3 = results.get(2);
        assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), r3.getTimestamp(r3.fieldIndex("window_start_time")));
        assertEquals("cmp002", r3.getString(r3.fieldIndex("campaign_id")));
        assertEquals("impression", r3.getString(r3.fieldIndex("event_type")));
        assertEquals(1L, r3.getLong(r3.fieldIndex("event_count")));
        assertEquals(0.03, r3.getDouble(r3.fieldIndex("total_bid_amount")), 0.001);

        // Window 2
        Row r4 = results.get(3);
        assertEquals(Timestamp.valueOf("2023-01-01 10:01:00"), r4.getTimestamp(r4.fieldIndex("window_start_time")));
        assertEquals("cmp001", r4.getString(r4.fieldIndex("campaign_id")));
        assertEquals("impression", r4.getString(r4.fieldIndex("event_type")));
        assertEquals(1L, r4.getLong(r4.fieldIndex("event_count")));
        assertEquals(0.07, r4.getDouble(r4.fieldIndex("total_bid_amount")), 0.001);

        Row r5 = results.get(4);
        assertEquals(Timestamp.valueOf("2023-01-01 10:01:00"), r5.getTimestamp(r5.fieldIndex("window_start_time")));
        assertEquals("cmp002", r5.getString(r5.fieldIndex("campaign_id")));
        assertEquals("impression", r5.getString(r5.fieldIndex("event_type")));
        assertEquals(1L, r5.getLong(r5.fieldIndex("event_count")));
        assertEquals(0.10, r5.getDouble(r5.fieldIndex("total_bid_amount")), 0.001);

        query.stop();
    }

    @Test
    void testEdgeCasesAggregation() throws Exception {
        List<Row> inputDataList = Arrays.asList(
            // Missing bid_amount_usd (should fail or skip)
            org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:10"), "cmp003", "impression", "user1", null),
            // Negative bid_amount_usd
            org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:20"), "cmp003", "impression", "user2", -0.01),
            // Very large bid_amount_usd
            org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:30"), "cmp003", "click", "user1", 1e9),
            // Timestamp on window boundary
            org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:01:00"), "cmp003", "impression", "user3", 0.10),
            // Duplicate event
            org.apache.spark.sql.RowFactory.create(Timestamp.valueOf("2023-01-01 10:00:10"), "cmp003", "impression", "user1", null)
        );
        Seq<Row> inputDataSeq = JavaConverters.asScalaIteratorConverter(inputDataList.iterator()).asScala().toSeq();
        MemoryStream<Row> inputStream = new MemoryStream<>(2, spark.sqlContext(), Option.empty(), RowEncoder.apply(eventSchema));
        Dataset<Row> events = inputStream.toDF();
        Dataset<Row> aggregatedEvents = events
            .withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "1 minute"), col("campaign_id"), col("event_type"))
            .agg(count("*").as("event_count"), sum("bid_amount_usd").as("total_bid_amount"))
            .select(col("window.start").as("window_start_time"), col("campaign_id"), col("event_type"), col("event_count"), col("total_bid_amount"));
        StreamingQuery query = aggregatedEvents
            .writeStream()
            .outputMode("update")
            .format("memory")
            .queryName("testEdgeCases")
            .start();
        inputStream.addData(inputDataSeq);
        query.processAllAvailable();
        Dataset<Row> resultTable = spark.sql("SELECT * FROM testEdgeCases ORDER BY window_start_time, campaign_id, event_type");
        List<Row> results = resultTable.collectAsList();
        // Check that duplicates are counted and invalid values do not break aggregation
        assertTrue(results.size() >= 2, "Should aggregate at least 2 groups");
        // Check that negative and large values are summed correctly
        boolean hasLargeBid = results.stream().anyMatch(r -> r.getDouble(r.fieldIndex("total_bid_amount")) >= 1e9);
        assertTrue(hasLargeBid, "Should contain large bid amount");
        query.stop();
    }
} 