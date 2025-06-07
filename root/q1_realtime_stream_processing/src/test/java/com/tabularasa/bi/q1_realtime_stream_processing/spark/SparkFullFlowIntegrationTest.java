package com.tabularasa.bi.q1_realtime_stream_processing.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class SparkFullFlowIntegrationTest {

    private static SparkSession spark;
    private static final String KAFKA_TOPIC = "ad-events-integration-test";

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.2"));

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
        spark = SparkSession.builder()
                .appName("SparkFullFlowIntegrationTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.streaming.checkpointLocation", "target/it-checkpoint-dir-" + System.currentTimeMillis())
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
    void testKafkaToSparkToPostgresFlow() throws Exception {
        // Kafka Producer Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Send test data to Kafka
            // (timestamp, campaign_id, event_type, user_id, bid_amount_usd)
            List<String> testEvents = Arrays.asList(
                    "{\"timestamp\":\"2023-01-01T10:00:10.000Z\",\"campaign_id\":\"it_cmp001\",\"event_type\":\"impression\",\"user_id\":\"user1\",\"bid_amount_usd\":0.05}",
                    "{\"timestamp\":\"2023-01-01T10:00:20.000Z\",\"campaign_id\":\"it_cmp001\",\"event_type\":\"impression\",\"user_id\":\"user2\",\"bid_amount_usd\":0.06}",
                    "{\"timestamp\":\"2023-01-01T10:00:30.000Z\",\"campaign_id\":\"it_cmp001\",\"event_type\":\"click\",\"user_id\":\"user1\",\"bid_amount_usd\":0.0}"
            );
            for (String event : testEvents) {
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, event));
            }
            producer.flush();
        }

        // Spark Streaming logic (adapted from AdEventsSparkStreamer)
        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka.getBootstrapServers())
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest") // Read from the beginning for test
                .load()
                .selectExpr("CAST(value AS STRING) as json_payload")
                .select(from_json(col("json_payload"), eventSchema).as("data"))
                .select("data.*");

        Dataset<Row> aggregatedEvents = kafkaStream
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

        // Write to PostgreSQL using foreachBatch (simplified AdEventDBSink logic)
        StreamingQuery query = aggregatedEvents
                .writeStream()
                .outputMode("update")
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS)) // Process quickly for test
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) return;
                    batchDF.write()
                            .format("jdbc")
                            .option("url", postgres.getJdbcUrl())
                            .option("dbtable", "aggregated_campaign_stats")
                            .option("user", postgres.getUsername())
                            .option("password", postgres.getPassword())
                            .option("driver", "org.postgresql.Driver")
                            .mode("append") // In a real scenario, this should be an upsert
                            .save();
                })
                .start();

        // Let the stream run for a bit to process data
        assertTrue(query.awaitTermination(60000), "Streaming query did not terminate in time"); // Wait for 1 minute or until terminated
        // Or use query.processAllAvailable() if the source is bounded or for more deterministic tests, but Kafka is continuous.
        // For this test, sending a finite amount of data and waiting for it to be processed is okay.
        // We rely on processing time trigger and awaitTermination.

        // Verify data in PostgreSQL
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT window_start_time, campaign_id, event_type, event_count, total_bid_amount FROM aggregated_campaign_stats ORDER BY window_start_time, campaign_id, event_type");

            Map<String, RowData> resultsMap = new HashMap<>();
            while (rs.next()) {
                RowData row = new RowData(
                        rs.getTimestamp("window_start_time"),
                        rs.getString("campaign_id"),
                        rs.getString("event_type"),
                        rs.getLong("event_count"),
                        rs.getDouble("total_bid_amount")
                );
                resultsMap.put(row.campaign_id + "_" + row.event_type, row);
                 System.out.println("DB Result: " + row);
            }

            assertEquals(2, resultsMap.size(), "Should have 2 aggregated groups in DB");

            RowData impressionData = resultsMap.get("it_cmp001_impression");
            assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), impressionData.window_start_time);
            assertEquals(2L, impressionData.event_count);
            assertEquals(0.11, impressionData.total_bid_amount, 0.001);

            RowData clickData = resultsMap.get("it_cmp001_click");
            assertEquals(Timestamp.valueOf("2023-01-01 10:00:00"), clickData.window_start_time);
            assertEquals(1L, clickData.event_count);
            assertEquals(0.0, clickData.total_bid_amount, 0.001);

        } finally {
            if(query.isActive()) query.stop();
        }
    }

    // Helper class for storing row data from DB for easier assertion
    private static class RowData {
        Timestamp window_start_time;
        String campaign_id;
        String event_type;
        long event_count;
        double total_bid_amount;

        public RowData(Timestamp window_start_time, String campaign_id, String event_type, long event_count, double total_bid_amount) {
            this.window_start_time = window_start_time;
            this.campaign_id = campaign_id;
            this.event_type = event_type;
            this.event_count = event_count;
            this.total_bid_amount = total_bid_amount;
        }
        @Override public String toString() { return String.format("Start: %s, Campaign: %s, Type: %s, Count: %d, Bid: %.2f", window_start_time, campaign_id, event_type, event_count, total_bid_amount); }
    }
} 