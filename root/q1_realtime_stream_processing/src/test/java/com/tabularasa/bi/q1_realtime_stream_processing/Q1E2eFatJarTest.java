package com.tabularasa.bi.q1_realtime_stream_processing;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Frame;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.github.dockerjava.api.async.ResultCallback;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Q1E2eFatJarTest {

    private static final Logger logger = LoggerFactory.getLogger(Q1E2eFatJarTest.class);
    private static File DOCKER_COMPOSE_FILE;

    private static final String SPARK_MASTER_SERVICE = "spark-master";
    private static final String KAFKA_SERVICE = "kafka";
    private static final String POSTGRES_SERVICE = "tabularasa_postgres_db";

    public static DockerComposeContainer<?> environment;

    @BeforeAll
    static void setup() throws Exception {
        DOCKER_COMPOSE_FILE = new File(Q1E2eFatJarTest.class.getClassLoader().getResource("docker-compose.test.yml").getPath());
        environment = new DockerComposeContainer<>(DOCKER_COMPOSE_FILE)
                .withExposedService(POSTGRES_SERVICE, 5432, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService(KAFKA_SERVICE, 9092)
                .withExposedService(SPARK_MASTER_SERVICE, 8081, Wait.forHttp("/").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(120)));
        environment.start();
        buildSparkJobJar();
        prepareDatabase();
    }

    @AfterAll
    static void teardown() {
        if (environment != null) {
            environment.stop();
        }
    }

    static void buildSparkJobJar() throws Exception {
        logger.info("Building Spark application JAR...");
        File projectDir = new File(new File("").getAbsolutePath());
        ProcessBuilder processBuilder = new ProcessBuilder("mvn", "clean", "package", "-DskipTests", "-f", "pom.xml");
        processBuilder.directory(projectDir);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Maven build failed with exit code: " + exitCode);
        }
        logger.info("Spark application JAR built successfully.");
    }

    static void prepareDatabase() throws Exception {
        logger.info("Preparing database schema...");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/airflow", environment.getServiceHost(POSTGRES_SERVICE, 5432), environment.getServicePort(POSTGRES_SERVICE, 5432));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "airflow", "airflow"); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS aggregated_campaign_stats;");
            stmt.execute("CREATE TABLE aggregated_campaign_stats (campaign_id VARCHAR(255), window_start_time TIMESTAMP, event_type VARCHAR(50), event_count BIGINT, total_bid_amount DECIMAL(18, 6), updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (campaign_id, window_start_time, event_type));");
            logger.info("Database schema prepared successfully.");
        }
    }

    @Test
    @Order(1)
    void submitSparkJobAndProduceData() throws Exception {
        logger.info("Submitting Spark job...");
        String sparkContainerId = environment.getContainerByServiceName(SPARK_MASTER_SERVICE).orElseThrow().getContainerId();

        String jarName = "q1_realtime_stream_processing-spark-job-0.0.1-SNAPSHOT.jar";
        String jarPath = "target/" + jarName;
        DockerClientFactory.instance().client().copyArchiveToContainerCmd(sparkContainerId)
                .withHostResource(jarPath)
                .withRemotePath("/opt/spark_apps/")
                .exec();

        String sparkSubmitCommand = String.format(
                "spark-submit --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer " +
                        "--master spark://spark-master:7077 " +
                        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.6.0 " +
                        "/opt/spark_apps/%s kafka:9092 ad-events jdbc:postgresql://tabularasa_postgres_db:5432/airflow airflow airflow", jarName);

        ExecCreateCmdResponse execCreateCmdResponse = DockerClientFactory.instance().client().execCreateCmd(sparkContainerId)
                .withCmd("sh", "-c", sparkSubmitCommand)
                .withAttachStdout(true).withAttachStderr(true).exec();

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                DockerClientFactory.instance().client().execStartCmd(execCreateCmdResponse.getId())
                        .exec(new ResultCallback.Adapter<Frame>() {
                            @Override
                            public void onNext(Frame item) {
                                logger.info("SPARK: {}", item.toString());
                            }
                        }).awaitCompletion();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        logger.info("Spark job submitted. Waiting 20 seconds for initialization...");
        TimeUnit.SECONDS.sleep(20);

        logger.info("Producing data to Kafka...");
        String kafkaContainerId = environment.getContainerByServiceName(KAFKA_SERVICE).orElseThrow().getContainerId();
        String sampleData = "{\\\"timestamp\\\":\\\"2024-05-15T10:00:15Z\\\",\\\"campaign_id\\\":\\\"cmp_1\\\",\\\"event_type\\\":\\\"impression\\\",\\\"user_id\\\":\\\"usr_a\\\",\\\"bid_amount_usd\\\":0.05}\\n" +
                "{\\\"timestamp\\\":\\\"2024-05-15T10:00:25Z\\\",\\\"campaign_id\\\":\\\"cmp_1\\\",\\\"event_type\\\":\\\"impression\\\",\\\"user_id\\\":\\\"usr_b\\\",\\\"bid_amount_usd\\\":0.06}\\n" +
                "{\\\"timestamp\\\":\\\"2024-05-15T10:00:45Z\\\",\\\"campaign_id\\\":\\\"cmp_1\\\",\\\"event_type\\\":\\\"click\\\",\\\"user_id\\\":\\\"usr_a\\\",\\\"bid_amount_usd\\\":0.10}";
        String kafkaCommand = "echo -e '" + sampleData + "' | kafka-console-producer.sh --bootstrap-server kafka:9092 --topic ad-events";

        ExecCreateCmdResponse kafkaExec = DockerClientFactory.instance().client().execCreateCmd(kafkaContainerId)
                .withCmd("sh", "-c", kafkaCommand).exec();
        DockerClientFactory.instance().client().execStartCmd(kafkaExec.getId()).exec(new ResultCallback.Adapter<>());

        logger.info("Data produced. Waiting 60 seconds for processing...");
        TimeUnit.SECONDS.sleep(60);
    }

    @Test
    @Order(2)
    void verifyResultsInDatabase() throws Exception {
        logger.info("Verifying results in PostgreSQL...");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/airflow", environment.getServiceHost(POSTGRES_SERVICE, 5432), environment.getServicePort(POSTGRES_SERVICE, 5432));
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
        logger.info("Verification successful!");
    }
} 