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

import java.util.concurrent.TimeUnit;

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
            System.exit(1);
        }
    }

    @Bean
    @Profile("spark")
    public CommandLineRunner sparkRunner(AdEventSparkStreamer streamer) {
        return args -> {
            log.info("Spark profile is active. Launching Spark streaming job via Spring context.");
            
            int maxRetries = 3;
            int retryCount = 0;
            boolean success = false;
            
            while (!success && retryCount < maxRetries) {
                try {
                    log.info("Initializing Spark streaming pipeline... (attempt {}/{})", retryCount + 1, maxRetries);
                    streamer.startStream();
                    log.info("Spark streaming job successfully launched and running");
                    success = true;
                    
                    // Регистрируем shutdown hook для корректного завершения
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        log.info("Application shutdown detected, stopping Spark streaming job...");
                        try {
                            streamer.stopStream();
                        } catch (Exception e) {
                            log.error("Error during Spark streaming shutdown", e);
                        }
                    }));
                } catch (Exception e) {
                    retryCount++;
                    log.error("Failed to start Spark streaming job (attempt {}/{}): {}", 
                             retryCount, maxRetries, e.getMessage());
                    
                    if (retryCount < maxRetries) {
                        // Экспоненциальное увеличение времени ожидания перед повторной попыткой
                        long waitTime = (long) Math.pow(2, retryCount) * 1000;
                        log.info("Waiting {} seconds before retrying...", waitTime / 1000);
                        TimeUnit.MILLISECONDS.sleep(waitTime);
                    } else {
                        log.error("Maximum retry attempts reached. Failed to start Spark streaming job.");
                        System.exit(1);
                    }
                }
            }
        };
    }

    @Bean
    @Profile("simple")
    public CommandLineRunner simpleRunner(AdEventsSimpleProcessor processor) {
        return args -> {
            log.info("Simple profile is active. Initializing Kafka Listener.");
            processor.startProcessing();
            
            // Регистрируем shutdown hook для корректного завершения
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Application shutdown detected, stopping Kafka event processing...");
                try {
                    processor.stopProcessing();
                } catch (Exception e) {
                    log.error("Error during Kafka processor shutdown", e);
                }
            }));
        };
    }
}