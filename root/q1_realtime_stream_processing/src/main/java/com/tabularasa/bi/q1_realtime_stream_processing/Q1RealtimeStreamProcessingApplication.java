package com.tabularasa.bi.q1_realtime_stream_processing;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/**
 * Main entry point for the Q1 Realtime Stream Processing application.
 */
@SpringBootApplication
public class Q1RealtimeStreamProcessingApplication {

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Q1RealtimeStreamProcessingApplication.class);

    /**
     * Main method to run the Spring Boot application.
     *
     * @param args Command line arguments.
     */
    public static void main(final String[] args) {
        try {
            // Set critical system properties for Hadoop/Spark
            System.setProperty("hadoop.home.dir", "/tmp");
            System.setProperty("java.security.manager", "allow");
            System.setProperty("user.home", "/tmp");
            System.setProperty("user.name", "root");
            System.setProperty("HADOOP_USER_NAME", "root");
            
            SpringApplication.run(Q1RealtimeStreamProcessingApplication.class, args);
        } catch (Exception e) {
            LOGGER.error("Critical error during application startup", e);
            System.exit(1);
        }
    }

    /**
     * Checks Spark availability and logs status.
     * This bean is only active when the 'spark' profile is enabled.
     *
     * @param sparkSession The Spark session.
     * @return A CommandLineRunner bean.
     */
    @Bean
    @Profile("spark")
    public CommandLineRunner checkSparkAvailability(final SparkSession sparkSession) {
        return args -> {
            if (sparkSession == null) {
                LOGGER.warn("=====================================");
                LOGGER.warn("WARNING: SparkSession is not initialized!");
                LOGGER.warn("Spark stream processing will NOT be available.");
                LOGGER.warn("Only REST API and Kafka Producer features will be available.");
                LOGGER.warn("=====================================");
            } else {
                LOGGER.info("SparkSession initialized successfully.");
                LOGGER.info("Spark version: {}", sparkSession.version());
                LOGGER.info("Spark running in mode: {}", sparkSession.sparkContext().master());
            }
        };
    }
}