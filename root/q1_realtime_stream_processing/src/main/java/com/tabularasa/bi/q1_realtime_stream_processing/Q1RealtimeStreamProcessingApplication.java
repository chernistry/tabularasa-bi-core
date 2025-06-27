package com.tabularasa.bi.q1_realtime_stream_processing;

import com.tabularasa.bi.q1_realtime_stream_processing.service.AdEventSparkStreamer;
import com.tabularasa.bi.q1_realtime_stream_processing.service.AdEventsSimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Arrays;

/**
 * Main application class for the Q1 real-time stream processing module.
 * This application can run in one of two modes, determined by the "spring.profiles.active" property:
 * 1.  "simple": A lightweight mode using Spring Kafka listeners and in-memory aggregation.
 *               This mode is suitable for simple use cases or environments where a full Spark cluster is not available.
 * 2.  "spark":   A distributed processing mode that launches a Spark Streaming job to handle high-volume event streams.
 *               This mode requires a running Spark cluster and is designed for scalability and fault tolerance.
 *
 * The application determines the active mode at startup and initializes the appropriate components.
 */
@SpringBootApplication
@EnableScheduling
public class Q1RealtimeStreamProcessingApplication {

    private static final Logger log = LoggerFactory.getLogger(Q1RealtimeStreamProcessingApplication.class);

    @Autowired
    private Environment env;

    public static void main(String[] args) {
        try {
            SpringApplication.run(Q1RealtimeStreamProcessingApplication.class, args);
            log.info("Application started successfully!");
        } catch (Exception e) {
            log.error("Critical error during application startup", e);
            // Ensure the application exits on a critical startup error
            System.exit(1);
        }
    }

    /**
     * A CommandLineRunner that runs on application startup and determines which processing mode to activate.
     *
     * @param context The application context.
     * @return A CommandLineRunner bean.
     */
    @Bean
    public CommandLineRunner commandLineRunner(ConfigurableApplicationContext context) {
        return args -> {
            String[] profiles = env.getActiveProfiles();
            if (Arrays.asList(profiles).contains("spark")) {
                log.info("SPARK profile is active. Spark Streamer will be started automatically.");
            } else {
                log.info("SIMPLE profile is active. Starting Simple Processor...");
                AdEventsSimpleProcessor simpleProcessor = context.getBean(AdEventsSimpleProcessor.class);
                simpleProcessor.startProcessing();
            }
        };
    }
}