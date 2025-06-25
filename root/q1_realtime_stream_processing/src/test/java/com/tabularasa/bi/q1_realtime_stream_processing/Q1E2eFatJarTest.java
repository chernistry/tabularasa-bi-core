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
import java.sql.ResultSetMetaData;
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
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Q1E2eFatJarTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Q1E2eFatJarTest.class);
    private static Network network = Network.newNetwork();

    @Container
    private static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14.11-alpine")
            .withDatabaseName("tabularasadb")
            .withUsername("tabulauser")
            .withPassword("tabulapass")
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
            .withNetworkAliases("spark-master")
            .withEnv("HADOOP_USER_NAME", "root")
            .withEnv("HOME", "/tmp")
            .withEnv("USER_HOME", "/tmp");

    @Container
    private static GenericContainer<?> sparkWorker = new GenericContainer<>(DockerImageName.parse("bitnami/spark:3.5"))
            .withExposedPorts(8081)
            .withEnv("SPARK_MODE", "worker")
            .withEnv("SPARK_MASTER_URL", "spark://spark-master:7077")
            .withNetwork(network)
            .withNetworkAliases("spark-worker")
            .dependsOn(sparkMaster)
            .withEnv("HADOOP_USER_NAME", "root")
            .withEnv("HOME", "/tmp")
            .withEnv("USER_HOME", "/tmp");

    // Test events to send to Kafka
    private static final String[] TEST_EVENTS = {
        "{\"timestamp\":\"2024-05-15T10:00:15Z\",\"campaign_id\":\"cmp_1\",\"event_type\":\"impression\",\"user_id\":\"usr_a\",\"bid_amount_usd\":0.05}",
        "{\"timestamp\":\"2024-05-15T10:00:25Z\",\"campaign_id\":\"cmp_1\",\"event_type\":\"impression\",\"user_id\":\"usr_b\",\"bid_amount_usd\":0.06}",
        "{\"timestamp\":\"2024-05-15T10:00:45Z\",\"campaign_id\":\"cmp_1\",\"event_type\":\"click\",\"user_id\":\"usr_a\",\"bid_amount_usd\":0.10}"
    };

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
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/tabularasadb", postgres.getHost(), postgres.getMappedPort(5432));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "tabulauser", "tabulapass"); Statement stmt = conn.createStatement()) {
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

        // Create checkpoint directory in Spark container
        ExecCreateCmdResponse mkdirCmd = DockerClientFactory.instance().client().execCreateCmd(sparkContainerId)
                .withCmd("sh", "-c", "mkdir -p /tmp/spark_checkpoints && mkdir -p /tmp/.ivy2")
                .exec();
        DockerClientFactory.instance().client().execStartCmd(mkdirCmd.getId()).exec(new ResultCallback.Adapter<>());
        LOGGER.info("Created checkpoint and ivy directories in Spark container");

        // Ensure /etc/passwd has entry for current UID (workaround for JDK 23 UnixPrincipal NPE)
        ExecCreateCmdResponse passwdFixCmd = DockerClientFactory.instance().client().execCreateCmd(sparkContainerId)
                .withCmd("sh", "-c", "getent passwd $(id -u) || echo \"spark:$(openssl passwd -1 spark):$(id -u):$(id -g):Spark User:/home/spark:/sbin/nologin\" >> /etc/passwd")
                .exec();
        DockerClientFactory.instance().client().execStartCmd(passwdFixCmd.getId()).exec(new ResultCallback.Adapter<>());
        LOGGER.info("Patched /etc/passwd in Spark container if necessary");

        // Copy JAR file to Spark container
        try {
            sparkMaster.copyFileToContainer(MountableFile.forHostPath(jarPath), "/opt/spark_apps/" + jarName);
            LOGGER.info("Copied JAR file to Spark container: {}", jarPath);
        } catch (Exception e) {
            LOGGER.error("Failed to copy JAR to Spark container", e);
            throw new RuntimeException("Failed to copy JAR to Spark container", e);
        }

        // Ensure Kafka topic exists before launching Spark job
        String kafkaContainerId = kafka.getContainerId();
        ExecCreateCmdResponse topicCreateExec = DockerClientFactory.instance().client().execCreateCmd(kafkaContainerId)
                .withCmd("sh", "-c", "kafka-topics.sh --create --topic ad-events --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092 --if-not-exists")
                .exec();
        DockerClientFactory.instance().client().execStartCmd(topicCreateExec.getId()).exec(new ResultCallback.Adapter<>());
        LOGGER.info("Kafka topic 'ad-events' created or verified");
        
        // Give Kafka a moment to setup the topic
        TimeUnit.SECONDS.sleep(5);

        // Launch Spark job with verbose output and checkpoint directory
        String sparkSubmitCommand = String.format(
                "export HADOOP_USER_NAME=root; spark-submit --verbose " +
                        "--class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer " +
                        "--master spark://spark-master:7077 " +
                        "--conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-debug.properties -Duser.home=/tmp -Duser.name=root' " +
                        "--conf 'spark.executor.extraJavaOptions=-Duser.home=/tmp -Duser.name=root' " +
                        "--conf spark.hadoop.fs.defaultFS=file:/// " +
                        "--conf spark.hadoop.hadoop.security.authentication=simple " +
                        "--conf spark.streaming.checkpointLocation=/tmp/spark_checkpoints " +
                        "--conf spark.ui.enabled=true " +
                        "--conf spark.jars.ivy=/tmp/.ivy2 " +
                        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.3 " +
                        "/opt/spark_apps/%s kafka:9092 ad-events jdbc:postgresql://postgres:5432/tabularasadb tabulauser tabulapass", jarName);

        // Launch the Spark job and capture its output
        ExecCreateCmdResponse execCreateCmdResponse = DockerClientFactory.instance().client().execCreateCmd(sparkContainerId)
                .withCmd("sh", "-c", sparkSubmitCommand)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .exec();

        StringBuilder sparkOutput = new StringBuilder();
        DockerClientFactory.instance().client().execStartCmd(execCreateCmdResponse.getId())
                .exec(new ResultCallback.Adapter<Frame>() {
                    @Override
                    public void onNext(Frame frame) {
                        String output = new String(frame.getPayload());
                        sparkOutput.append(output);
                        LOGGER.info("SPARK_OUTPUT: {}", output);
                        super.onNext(frame);
                    }
                })
                .awaitStarted();

        LOGGER.info("Spark job submitted. Waiting 30 seconds for initialization...");
        TimeUnit.SECONDS.sleep(30);

        LOGGER.info("Producing data to Kafka...");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig())) {
            // Send some test data to Kafka
            for (String event : TEST_EVENTS) {
                LOGGER.info("Sending event to Kafka: {}", event);
                producer.send(new ProducerRecord<>("ad-events", event)).get();
            }
            LOGGER.info("Data sent to Kafka");
        }

        LOGGER.info("Data produced. Waiting 120 seconds for processing...");
        TimeUnit.SECONDS.sleep(120); // Increased to 2 minutes to give Spark more time to process
        
        // Log Spark driver output after job has had time to run
        LOGGER.info("Spark job output: {}", sparkOutput);
    }

    @Test
    @Order(2)
    void verifyResultsInDatabase() throws Exception {
        LOGGER.info("Verifying results in PostgreSQL...");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/tabularasadb", postgres.getHost(), postgres.getMappedPort(5432));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "tabulauser", "tabulapass"); 
             Statement stmt = conn.createStatement()) {
            
            // Check if table exists
            ResultSet tablesRs = conn.getMetaData().getTables(null, null, "aggregated_campaign_stats", null);
            boolean tableExists = tablesRs.next();
            LOGGER.info("Table 'aggregated_campaign_stats' exists: {}", tableExists);
            
            if (tableExists) {
                LOGGER.info("Table 'aggregated_campaign_stats' exists, checking contents...");
                
                // Dump the entire table for debugging
                ResultSet dumpRs = stmt.executeQuery("SELECT * FROM aggregated_campaign_stats");
                ResultSetMetaData metaData = dumpRs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                int rowCount = 0;
                while (dumpRs.next()) {
                    rowCount++;
                    StringBuilder rowData = new StringBuilder("Row " + rowCount + ": ");
                    for (int i = 1; i <= columnCount; i++) {
                        rowData.append(metaData.getColumnName(i))
                              .append("=")
                              .append(dumpRs.getString(i))
                              .append(", ");
                    }
                    LOGGER.info(rowData.toString());
                }
                LOGGER.info("Found {} rows in aggregated_campaign_stats", rowCount);
                
                // Now check specific data we expect
                ResultSet rs = stmt.executeQuery("SELECT * FROM aggregated_campaign_stats WHERE event_type = 'impression'");
                boolean hasImpressions = rs.next();
                if (!hasImpressions) {
                    LOGGER.error("No results found in the database table for impressions!");
                }
                assertTrue(hasImpressions, "Should have results for impressions.");
                
                ResultSet rsClicks = stmt.executeQuery("SELECT * FROM aggregated_campaign_stats WHERE event_type = 'click'");
                boolean hasClicks = rsClicks.next();
                if (!hasClicks) {
                    LOGGER.error("No results found in the database table for clicks!");
                }
                assertTrue(hasClicks, "Should have results for clicks.");
            } else {
                LOGGER.error("Table 'aggregated_campaign_stats' does not exist!");
                fail("Database table 'aggregated_campaign_stats' does not exist");
            }
        }
    }

    /**
     * Helper method to create Kafka producer configuration
     */
    private Properties producerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}