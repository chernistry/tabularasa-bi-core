package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for AdEventsSparkStreamer using file source.
 * Tests the complete flow from reading JSON events to writing aggregated data to PostgreSQL.
 */
@Testcontainers
public class AdEventsSparkStreamerIntegrationTest {

    private static SparkSession spark;
    private static final String TEST_FILE_PATH = "src/test/resources/test_ad_events.jsonl";

    @Container
    @SuppressWarnings("resource")
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13.3")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    private static final StructType eventSchema = new StructType()
            .add("timestamp", DataTypes.TimestampType, false)
            .add("campaign_id", DataTypes.StringType, false)
            .add("event_type", DataTypes.StringType, false)
            .add("user_id", DataTypes.StringType, false)
            .add("bid_amount_usd", DataTypes.DoubleType, false);

    @BeforeAll
    static void setup() throws Exception {
        // Create test directory and ensure test file exists
        Path testDir = Paths.get(TEST_FILE_PATH).getParent();
        if (!Files.exists(testDir)) {
            Files.createDirectories(testDir);
        }

        // Initialize Spark
        spark = SparkSession.builder()
                .appName("AdEventsSparkStreamerIntegrationTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.streaming.checkpointLocation", "target/test-checkpoint-dir-" + System.currentTimeMillis())
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        // Create table in PostgreSQL
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            String createTableSql = "CREATE TABLE aggregated_campaign_stats (" +
                    " id SERIAL PRIMARY KEY," +
                    " window_start_time TIMESTAMP NOT NULL," +
                    " campaign_id VARCHAR(255) NOT NULL," +
                    " event_type VARCHAR(50) NOT NULL," +
                    " event_count BIGINT NOT NULL," +
                    " total_bid_amount DOUBLE PRECISION NOT NULL," +
                    " updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    " UNIQUE (window_start_time, campaign_id, event_type));";
            stmt.execute(createTableSql);
        }
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void testFileSourceToPostgresFlow() throws Exception {
        // Read from test file
        Dataset<Row> events = spark
                .readStream()
                .schema(eventSchema)
                .json(TEST_FILE_PATH);

        // Apply windowing and aggregation
        Dataset<Row> aggregatedEvents = events
                .withWatermark("timestamp", "10 seconds")
                .groupBy(
                        window(col("timestamp"), "1 minute"),
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

        // Write to PostgreSQL
        StreamingQuery query = aggregatedEvents
                .writeStream()
                .outputMode("update")
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) return;
                    batchDF.write()
                            .format("jdbc")
                            .option("url", postgres.getJdbcUrl())
                            .option("dbtable", "aggregated_campaign_stats")
                            .option("user", postgres.getUsername())
                            .option("password", postgres.getPassword())
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();
                })
                .start();

        // Wait for processing to complete
        query.processAllAvailable();
        query.stop();

        // Verify data in PostgreSQL
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT window_start_time, campaign_id, event_type, event_count, total_bid_amount FROM aggregated_campaign_stats ORDER BY window_start_time, campaign_id, event_type");

            Map<String, RowData> resultsMap = new HashMap<>();
            while (rs.next()) {
                RowData row = new RowData(
                        rs.getString("campaign_id"),
                        rs.getString("event_type"),
                        rs.getLong("event_count"),
                        rs.getDouble("total_bid_amount")
                );
                resultsMap.put(row.campaign_id + "_" + row.event_type, row);
            }

            assertEquals(4, resultsMap.size(), "Should have 4 aggregated groups in DB");

            // Check first window aggregations (10:00:00 - 10:01:00)
            RowData cmp1Impressions = resultsMap.get("test_cmp001_impression");
            assertNotNull(cmp1Impressions, "Should have test_cmp001 impressions");
            assertEquals(2L, cmp1Impressions.event_count);
            assertEquals(0.11, cmp1Impressions.total_bid_amount, 0.001);

            RowData cmp1Clicks = resultsMap.get("test_cmp001_click");
            assertNotNull(cmp1Clicks, "Should have test_cmp001 clicks");
            assertEquals(1L, cmp1Clicks.event_count);
            assertEquals(0.0, cmp1Clicks.total_bid_amount, 0.001);

            // Check second window aggregations (10:01:00 - 10:02:00)
            RowData cmp2Impressions = resultsMap.get("test_cmp002_impression");
            assertNotNull(cmp2Impressions, "Should have test_cmp002 impressions");
            assertEquals(2L, cmp2Impressions.event_count);
            assertEquals(0.15, cmp2Impressions.total_bid_amount, 0.001);

            RowData cmp2Conversions = resultsMap.get("test_cmp002_conversion");
            assertNotNull(cmp2Conversions, "Should have test_cmp002 conversions");
            assertEquals(1L, cmp2Conversions.event_count);
            assertEquals(0.0, cmp2Conversions.total_bid_amount, 0.001);
        }
    }

    // Helper class for storing row data from DB for easier assertion
    private static class RowData {
        String campaign_id;
        String event_type;
        long event_count;
        double total_bid_amount;

        public RowData(String campaign_id, String event_type, long event_count, double total_bid_amount) {
            this.campaign_id = campaign_id;
            this.event_type = event_type;
            this.event_count = event_count;
            this.total_bid_amount = total_bid_amount;
        }
    }
} 