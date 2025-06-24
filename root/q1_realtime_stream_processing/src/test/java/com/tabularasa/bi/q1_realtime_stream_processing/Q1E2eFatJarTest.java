package com.tabularasa.bi.q1_realtime_stream_processing;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Frame;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.github.dockerjava.api.async.ResultCallback;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.io.InputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Q1E2eFatJarTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Q1E2eFatJarTest.class);
    private static Network network = Network.newNetwork();

    @Container
    private static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14.11-alpine")
            .withDatabaseName("airflow")
            .withUsername("airflow")
            .withPassword("airflow")
            .withNetwork(network)
            .withNetworkAliases("postgres");

    @Container
    private static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka");

    @Container
    private static GenericContainer<?> sparkMaster = new GenericContainer<>(DockerImageName.parse("bitnami/spark:3.5"))
            .withExposedPorts(7077, 8080)
            .withEnv("SPARK_MODE", "master")
            .withNetwork(network)
            .withNetworkAliases("spark-master");

    @Container
    private static GenericContainer<?> sparkWorker = new GenericContainer<>(DockerImageName.parse("bitnami/spark:3.5"))
            .withExposedPorts(8081)
            .withEnv("SPARK_MODE", "worker")
            .withEnv("SPARK_MASTER_URL", "spark://spark-master:7077")
            .withNetwork(network)
            .withNetworkAliases("spark-worker")
            .dependsOn(sparkMaster);

    @BeforeAll
    public static void setup() {
        try {
            // Start containers with try-with-resources to ensure proper cleanup
            try {
                postgres.start();
                kafka.start();
                sparkMaster.start();
                sparkWorker.start();
            } catch (RuntimeException e) {
                LOGGER.error("Container startup failed", e);
                throw e;
            }

            // Set the JDBC URL, Kafka bootstrap servers, and Spark master URL dynamically
            System.setProperty("POSTGRES_URL", postgres.getJdbcUrl());
            System.setProperty("KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
            System.setProperty("SPARK_MASTER_URL", "spark://" + sparkMaster.getHost() + ":" + sparkMaster.getMappedPort(7077));

            buildSparkJobJar();
            prepareDatabase();
        } catch (Exception e) {
            LOGGER.error("Failed to set up test environment", e);
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void teardown() {
        sparkWorker.stop();
        sparkMaster.stop();
        kafka.stop();
        postgres.stop();
        network.close();
    }

    static void buildSparkJobJar() throws Exception {
        LOGGER.info("Building Spark application JAR...");
        File projectDir = new File(new File("").getAbsolutePath());
        ProcessBuilder processBuilder = new ProcessBuilder("mvn", "clean", "package", "-DskipTests", "-f", "pom.xml");
        processBuilder.directory(projectDir);
        
        Process process = processBuilder.start();
        try {
            // Read output streams to prevent resource leaks
            try (InputStream stdout = process.getInputStream();
                 InputStream stderr = process.getErrorStream()) {
                // Consume streams to prevent blocking
                new Thread(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stdout))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            LOGGER.info("MVN_BUILD_STDOUT: {}", line);
                        }
                    } catch (IOException e) {
                        LOGGER.error("Error reading stdout", e);
                    }
                }).start();
                
                new Thread(() -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stderr))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            LOGGER.error("MVN_BUILD_STDERR: {}", line);
                        }
                    } catch (IOException e) {
                        LOGGER.error("Error reading stderr", e);
                    }
                }).start();
                
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new RuntimeException("Maven build failed with exit code: " + exitCode);
                }
            }
        } finally {
            process.destroy();
        }
        LOGGER.info("Spark application JAR built successfully.");
    }

    static void prepareDatabase() throws Exception {
        LOGGER.info("Preparing database schema...");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/airflow", postgres.getHost(), postgres.getMappedPort(5432));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "airflow", "airflow"); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS aggregated_campaign_stats;");
            stmt.execute("CREATE TABLE aggregated_campaign_stats (campaign_id VARCHAR(255), window_start_time TIMESTAMP, event_type VARCHAR(50), event_count BIGINT, total_bid_amount DECIMAL(18, 6), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (campaign_id, window_start_time, event_type));");
            LOGGER.info("Database schema prepared successfully.");
        }
    }

    @Test
    @Order(1)
    void submitSparkJobAndProduceData() throws Exception {
        LOGGER.info("Submitting Spark job...");
        String sparkContainerId = sparkMaster.getContainerId();

        String jarName = "q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar";
        String jarPath = "target/" + jarName;

        // Copy JAR using Testcontainers helper (more robust than low-level Docker copy)
        try {
            sparkMaster.copyFileToContainer(MountableFile.forHostPath(jarPath), "/opt/spark_apps/" + jarName);
        } catch (Exception e) {
            LOGGER.error("Failed to copy JAR to Spark container", e);
            throw new RuntimeException("Failed to copy JAR to Spark container", e);
        }

        String sparkSubmitCommand = String.format(
                "spark-submit --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer " +
                        "--master spark://spark-master:7077 " +
                        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 " +
                        "/opt/spark_apps/%s kafka:9092 ad-events jdbc:postgresql://postgres:5432/airflow airflow airflow", jarName);

        ExecCreateCmdResponse execCreateCmdResponse = DockerClientFactory.instance().client().execCreateCmd(sparkContainerId)
                .withCmd("sh", "-c", sparkSubmitCommand)
                .withAttachStdout(true).withAttachStderr(true).exec();

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                DockerClientFactory.instance().client().execStartCmd(execCreateCmdResponse.getId())
                        .exec(new ResultCallback.Adapter<Frame>() {
                            @Override
                            public void onNext(Frame item) {
                                LOGGER.info("SPARK: {}", item.toString());
                            }
                        }).awaitCompletion();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        LOGGER.info("Spark job submitted. Waiting 20 seconds for initialization...");
        TimeUnit.SECONDS.sleep(20);

        LOGGER.info("Producing data to Kafka...");
        // Ensure Kafka topic exists (manually execute command instead of using createTopic)
        String kafkaContainerId = kafka.getContainerId();
        ExecCreateCmdResponse topicCreateExec = DockerClientFactory.instance().client().execCreateCmd(kafkaContainerId)
                .withCmd("sh", "-c", "kafka-topics.sh --create --topic ad-events --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092 --if-not-exists")
                .exec();
        DockerClientFactory.instance().client().execStartCmd(topicCreateExec.getId()).exec(new ResultCallback.Adapter<>());
        
        // Use KafkaContainer to get bootstrap servers address
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String[] events = {
                "{\"timestamp\":\"2024-05-15T10:00:15Z\",\"campaign_id\":\"cmp_1\",\"event_type\":\"impression\",\"user_id\":\"usr_a\",\"bid_amount_usd\":0.05}",
                "{\"timestamp\":\"2024-05-15T10:00:25Z\",\"campaign_id\":\"cmp_1\",\"event_type\":\"impression\",\"user_id\":\"usr_b\",\"bid_amount_usd\":0.06}",
                "{\"timestamp\":\"2024-05-15T10:00:45Z\",\"campaign_id\":\"cmp_1\",\"event_type\":\"click\",\"user_id\":\"usr_a\",\"bid_amount_usd\":0.10}"
            };
            
            for (String event : events) {
                LOGGER.info("Sending event to Kafka: {}", event);
                producer.send(new ProducerRecord<>("ad-events", "key-" + System.currentTimeMillis(), event)).get();
            }
            producer.flush();
            LOGGER.info("Data sent to Kafka");
        }

        LOGGER.info("Data produced. Waiting 60 seconds for processing...");
        TimeUnit.SECONDS.sleep(60);
    }

    @Test
    @Order(2)
    void verifyResultsInDatabase() throws Exception {
        LOGGER.info("Verifying results in PostgreSQL...");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/airflow", postgres.getHost(), postgres.getMappedPort(5432));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "airflow", "airflow"); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM aggregated_campaign_stats ORDER BY campaign_id, event_type");
            assertTrue(rs.next(), "Should have results for impressions.");
            assertEquals("cmp_1", rs.getString("campaign_id"));
            assertEquals("impression", rs.getString("event_type"));
            assertEquals(2, rs.getInt("event_count"));
            assertEquals(0.11, rs.getDouble("total_bid_amount"), 0.001);

            assertTrue(rs.next(), "Should have results for clicks.");
            assertEquals("cmp_1", rs.getString("campaign_id"));
            assertEquals("click", rs.getString("event_type"));
            assertEquals(1, rs.getInt("event_count"));
            assertEquals(0.10, rs.getDouble("total_bid_amount"), 0.001);
        }
        LOGGER.info("Verification successful!");
    }
}