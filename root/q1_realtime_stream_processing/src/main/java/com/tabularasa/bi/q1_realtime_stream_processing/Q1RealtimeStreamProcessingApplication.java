package com.tabularasa.bi.q1_realtime_stream_processing;

import com.tabularasa.bi.q1_realtime_stream_processing.service.AdEventSparkStreamer;
import com.tabularasa.bi.q1_realtime_stream_processing.service.AdEventsSimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

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

    public static void main(String[] args) {
        try {
            SpringApplication.run(Q1RealtimeStreamProcessingApplication.class, args);
        } catch (Exception e) {
            log.error("Critical error during application startup", e);
            // Optionally, force exit if startup fails critically
            // System.exit(1);
        }
    }

    @Bean
    @Profile("spark")
    public CommandLineRunner sparkRunner(AdEventSparkStreamer streamer) {
        return args -> {
            log.info("Spark profile is active. Launching Spark streaming job via Spring context.");
            try {
                streamer.startStream();
            } catch (Exception e) {
                log.error("Failed to start Spark streaming job", e);
                System.exit(1);
            }
        };
    }

    @Bean
    @Profile("simple")
    public CommandLineRunner simpleRunner(AdEventsSimpleProcessor processor) {
        return args -> {
            log.info("Simple profile is active. Initializing Kafka Listener.");
            processor.startProcessing();
        };
    }
}