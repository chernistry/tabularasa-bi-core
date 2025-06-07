package com.tabularasa.bi.q1_realtime_stream_processing;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
public class Q1RealtimeStreamProcessingApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(Q1RealtimeStreamProcessingApplication.class);

    public static void main(String[] args) {
        try {
            // System.setProperty("hadoop.home.dir", "/"); // Moved to SparkConfig
            // System.setProperty("java.security.manager", "allow"); // Moved to SparkConfig
            SpringApplication.run(Q1RealtimeStreamProcessingApplication.class, args);
        } catch (Exception e) {
            logger.error("Critical error during application startup", e);
            System.exit(1);
        }
    }

    /**
     * Checks Spark availability and logs status.
     * This bean is only active when the 'spark' profile is enabled.
     */
    @Bean
    @Profile("spark")
    public CommandLineRunner checkSparkAvailability(SparkSession sparkSession) {
        return args -> {
            if (sparkSession == null) {
                logger.warn("=====================================");
                logger.warn("WARNING: SparkSession is not initialized!");
                logger.warn("Spark stream processing will NOT be available.");
                logger.warn("Only REST API and Kafka Producer features will be available.");
                logger.warn("=====================================");
            } else {
                logger.info("SparkSession initialized successfully.");
                logger.info("Spark version: {}", sparkSession.version());
                logger.info("Spark running in mode: {}", sparkSession.sparkContext().master());
            }
        };
    }
} 