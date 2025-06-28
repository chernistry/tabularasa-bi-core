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
}

@Configuration
@Profile("spark")
@Slf4j
public class SparkConfig implements DisposableBean {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master:local[2]}")
    private String sparkMaster;

    @Value("${spark.driver.memory:1g}")
    private String driverMemory;

    @Value("${spark.executor.memory:1g}")
    private String executorMemory;

    @Value("${spark.executor.cores:1}")
    private int executorCores;

    @Value("${spark.default.parallelism:2}")
    private int defaultParallelism;

    @Value("${spark.sql.shuffle.partitions:2}")
    private int shufflePartitions;
    
    @Value("${spark.streaming.checkpoint-location:/tmp/spark_checkpoints}")
    private String checkpointLocation;
    
    @Value("${spark.kryo.registrator:com.tabularasa.bi.q1_realtime_stream_processing.serialization.KryoRegistrator}")
    private String kryoRegistrator;
    
    private SparkSession sparkSession;
    private JavaSparkContext javaSparkContext;

    @Bean
    @Primary
    public SparkSession sparkSession() {
        // Always use local mode for stability
        String actualMaster = sparkMaster;
        
        log.info("Creating SparkSession with app name: {}, master: {}, driverMemory: {}, executorMemory: {}", 
                appName, actualMaster, driverMemory, executorMemory);
        
        SparkConf conf = new SparkConf()
            .setAppName(appName)
            .setMaster(actualMaster)
            // Use Kryo serializer for better performance
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", kryoRegistrator)
            
            .set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .set("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))

            // Enable Adaptive Query Execution (AQE)
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .set("spark.sql.adaptive.skewJoin.enabled", "true")
            .set("spark.sql.adaptive.localShuffleReader.enabled", "true")

            // Resource settings
            .set("spark.driver.memory", driverMemory)
            .set("spark.executor.memory", executorMemory)
            .set("spark.executor.cores", String.valueOf(executorCores))

            // Scheduler settings
            .set("spark.scheduler.mode", "FAIR")
            
            // Backpressure for streaming
            .set("spark.streaming.backpressure.enabled", "true")
            
            // Network optimization
            .set("spark.network.timeout", "600s")
            
            // Memory optimization
            .set("spark.memory.fraction", "0.6")
            .set("spark.memory.storageFraction", "0.5")
            
            // Disable dynamic allocation for local/standalone mode for stability
            .set("spark.dynamicAllocation.enabled", "false")
            
            // Disable UI to resolve servlet API conflict issues
            .set("spark.ui.enabled", "false")
            
            .set("spark.streaming.stopGracefullyOnShutdown", "true")

            // Fix class loading issues
            .set("spark.driver.userClassPathFirst", "true")
            .set("spark.executor.userClassPathFirst", "true")
            
            // Set logging level through configuration
            .set("spark.log.level", "WARN");

        // Create SparkSession
        try {
            this.sparkSession = SparkSession.builder()
                .config(conf)
                .getOrCreate();
                
            return sparkSession;
        } catch (Exception e) {
            log.error("Failed to create SparkSession", e);
            throw new RuntimeException("Failed to initialize Spark", e);
        }
    }
    
    @Bean
    public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
        try {
            log.info("Creating JavaSparkContext from SparkSession");
            // Use existing SparkContext from SparkSession
            this.javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
            return javaSparkContext;
        } catch (Exception e) {
            log.error("Failed to create JavaSparkContext", e);
            throw new RuntimeException("Failed to initialize JavaSparkContext", e);
        }
    }
    
    @Override
    public void destroy() {
        log.info("Shutting down Spark resources");
        if (sparkSession != null && !sparkSession.sparkContext().isStopped()) {
            try {
                sparkSession.stop();
                sparkSession = null;
            } catch (Exception e) {
                log.error("Error stopping SparkSession", e);
            }
        }
    }
}
