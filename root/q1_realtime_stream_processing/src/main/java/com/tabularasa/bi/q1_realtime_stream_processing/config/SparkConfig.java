package com.tabularasa.bi.q1_realtime_stream_processing.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.jta.JtaAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import org.apache.spark.SparkContext;

/**
 * Separate Spring Boot application class for the Spark profile
 * This prevents JPA-related auto-configuration from loading
 */
@SpringBootApplication
@EnableScheduling
@EnableAsync
@ConfigurationPropertiesScan
@Profile("spark")
@EnableAutoConfiguration
class SparkApplication {
    // This class just serves as a container for Spring Boot configuration
    // The actual implementation is still in Q1RealtimeStreamProcessingApplication
    
    /**
     * Explicitly provide a ServletWebServerFactory bean for Tomcat
     */
    @Bean
    public ServletWebServerFactory servletWebServerFactory() {
        return new TomcatServletWebServerFactory();
    }
    
    static {
        // Disable Kerberos authentication to fix NullPointerException
        System.setProperty("java.security.krb5.conf", "/dev/null");
        System.setProperty("hadoop.security.authentication", "simple");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        
        // Completely disable Spark UI to avoid Jersey conflicts
        System.setProperty("spark.ui.enabled", "false");
        System.setProperty("spark.ui.port", "0");
        System.setProperty("spark.ui.showConsoleProgress", "false");
    }
}

@Configuration
@Profile("spark")
@Slf4j
public class SparkConfig implements DisposableBean {

    @Value("${spark.app.name:AdEventSparkStreamer}")
    private String appName;

    @Value("${spark.master.url:local[*]}")
    private String masterUrl;

    @Value("${spark.driver.memory:1g}")
    private String driverMemory;

    @Value("${spark.executor.memory:1g}")
    private String executorMemory;

    @Value("${spark.executor.cores:1}")
    private int executorCores;

    @Value("${spark.default.parallelism:2}")
    private int defaultParallelism;

    @Value("${spark.sql.shuffle.partitions:8}")
    private int shufflePartitions;
    
    @Value("${spark.streaming.checkpoint-location:/tmp/spark_checkpoints}")
    private String checkpointLocation;
    
    @Value("${spark.kryo.registrator:com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator}")
    private String kryoRegistrator;
    
    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;
    
    static {
        // Set system properties to fix serialization issues
        System.setProperty("sun.io.serialization.extendedDebugInfo", "true");
        System.setProperty("java.io.serialization.strict", "false");
        System.setProperty("jdk.serialFilter", "allow");
        System.setProperty("sun.serialization.validateClassSerialVersionUID", "false");
        
        // Completely disable Spark UI to avoid Jersey conflicts
        System.setProperty("spark.ui.enabled", "false");
        System.setProperty("spark.ui.port", "0");
        System.setProperty("spark.ui.showConsoleProgress", "false");
    }

    @Bean
    @Primary
    public SparkSession sparkSession() {
        if (sparkSession == null) {
            log.info("Creating SparkSession with app name: {}, master: {}, driverMemory: {}, executorMemory: {}",
                    appName, masterUrl, driverMemory, executorMemory);

            // Completely disable Spark UI to avoid Jersey conflicts
            System.setProperty("spark.ui.enabled", "false");
            System.setProperty("spark.ui.port", "4040");
            System.setProperty("spark.ui.showConsoleProgress", "false");

            SparkConf sparkConf = new SparkConf()
                    .setAppName(appName)
                    .setMaster(masterUrl)
                    .set("spark.driver.memory", driverMemory)
                    .set("spark.executor.memory", executorMemory)
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryo.registrator", kryoRegistrator)
                    .set("spark.kryo.registrationRequired", "false")
                    .set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
                    .set("spark.default.parallelism", String.valueOf(defaultParallelism))
                    .set("spark.sql.adaptive.enabled", "true")
                    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .set("spark.sql.adaptive.skewJoin.enabled", "true")
                    .set("spark.ui.enabled", "false")
                    .set("spark.ui.port", "4040")
                    .set("spark.ui.showConsoleProgress", "false")
                    .set("spark.sql.codegen.wholeStage", "false");

            try {
                // Attempt to create SparkSession with provided master URL
                sparkSession = SparkSession.builder()
                        .config(sparkConf)
                        .getOrCreate();
            } catch (Exception e) {
                log.warn("Failed to connect to Spark master {} – falling back to local[*]", masterUrl, e);
                // Retry with local master to ensure application can still start in standalone mode
                sparkConf.setMaster("local[*]");
                sparkSession = SparkSession.builder()
                        .config(sparkConf)
                        .getOrCreate();
            }
                    
            // Disable UI after creation as well
            sparkSession.sparkContext().uiWebUrl().isDefined();
        }
        return sparkSession;
    }
    
    @Bean
    public JavaSparkContext javaSparkContext() {
        if (javaSparkContext == null) {
            log.info("Creating JavaSparkContext from SparkSession");
            SparkContext sparkContext = sparkSession().sparkContext();
            javaSparkContext = new JavaSparkContext(sparkContext);
        }
        return javaSparkContext;
    }
    
    @Override
    public void destroy() {
        log.info("Shutting down Spark resources");
        if (javaSparkContext != null) {
            javaSparkContext.close();
            javaSparkContext = null;
        }
        if (sparkSession != null) {
            sparkSession.close();
            sparkSession = null;
        }
    }
}
