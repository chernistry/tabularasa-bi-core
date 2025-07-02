package com.tabularasa.bi.q1_realtime_stream_processing;

import com.tabularasa.bi.q1_realtime_stream_processing.service.AdEventSparkStreamer;
import com.tabularasa.bi.q1_realtime_stream_processing.service.AdEventsSimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.jta.JtaAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;

import java.util.Map;
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
@EnableAsync
@ConfigurationPropertiesScan
public class Q1RealtimeStreamProcessingApplication {

    private static final Logger log = LoggerFactory.getLogger(Q1RealtimeStreamProcessingApplication.class);

    public static void main(String[] args) {
        try {
            // Configure Jersey/Servlet compatibility before Spring context initialization
            System.setProperty("spark.ui.enabled", "false");
            System.setProperty("spark.ui.port", "4040");
            System.setProperty("spark.ui.showConsoleProgress", "false");
            System.setProperty("org.eclipse.jetty.server.Server.useJakartaServletApi", "true");
            
            // Disable Jersey servlet container initialization in Spark
            System.setProperty("spark.ui.enabled", "false");
            System.setProperty("spark.ui.port", "4040");
            
            SpringApplication.run(Q1RealtimeStreamProcessingApplication.class, args);
        } catch (Exception e) {
            log.error("Critical error during application startup", e);
            // Optionally, force exit if startup fails critically
            System.exit(1);
        }
    }

    @Bean
    public ThreadPoolTaskExecutor applicationTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("app-task-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(10);
        executor.initialize();
        return executor;
    }
    
    /**
     * Отключение проверки схемы для профиля spark, чтобы избежать конфликтов
     * между JPA и Spark при работе с базой данных.
     */
    @Bean
    @Profile("spark")
    public HibernatePropertiesCustomizer hibernatePropertiesCustomizer() {
        return hibernateProperties -> 
            hibernateProperties.put("hibernate.hbm2ddl.auto", "none");
    }
    
    /**
     * Initialize database schema before application starts
     */
    @Bean
    public ApplicationRunner databaseInitializer(JdbcTemplate jdbcTemplate) {
        return args -> {
            log.info("Checking database schema...");
            
            // TODO: Use a proper database migration tool like Flyway or Liquibase
            // to manage schema changes versionally and avoid manual DDL in code.
            // Dropping views like this is a temporary workaround.
            boolean viewExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'v_aggregated_campaign_stats')", 
                Boolean.class);
                
            if (viewExists) {
                log.info("Found view v_aggregated_campaign_stats that depends on our table. Dropping it to prevent schema conflicts.");
                jdbcTemplate.execute("DROP VIEW IF EXISTS v_aggregated_campaign_stats");
                log.info("View dropped successfully");
            }
            
            // TODO: DDL should be externalized into SQL migration scripts.
            boolean tableExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'aggregated_campaign_stats')", 
                Boolean.class);
                
            if (!tableExists) {
                log.info("Creating aggregated_campaign_stats table");
                jdbcTemplate.execute(
                    "CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (" +
                    "id SERIAL PRIMARY KEY, " +
                    "campaign_id VARCHAR(255) NOT NULL, " +
                    "event_type VARCHAR(50) NOT NULL, " +
                    "window_start_time TIMESTAMP NOT NULL, " +
                    "window_end_time TIMESTAMP NOT NULL, " +
                    "event_count BIGINT NOT NULL, " +
                    "total_bid_amount DECIMAL(18, 6) NOT NULL, " +
                    "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
                    "CONSTRAINT aggregated_campaign_stats_unique UNIQUE (campaign_id, event_type, window_start_time)" +
                    ")"
                );
                
                // Create indexes for better performance
                jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_aggregated_campaign_stats_campaign_id ON aggregated_campaign_stats (campaign_id)");
                jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_aggregated_campaign_stats_event_type ON aggregated_campaign_stats (event_type)");
                jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_aggregated_campaign_stats_window ON aggregated_campaign_stats (window_start_time, window_end_time)");
                
                log.info("Table and indexes created successfully");
            }
        };
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
                    
                    // Register shutdown hook for proper termination
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
                             retryCount, maxRetries, e.getMessage(), e);
                    
                    if (retryCount < maxRetries) {
                        // Exponential backoff before retrying
                        long waitTime = (long) Math.pow(2, retryCount) * 1000;
                        log.info("Waiting {} seconds before retrying...", waitTime / 1000);
                        TimeUnit.MILLISECONDS.sleep(waitTime);
                    } else {
                        log.error("Maximum retry attempts reached. Failed to start Spark streaming job.");
                        // The application will continue to run to keep other potential services (e.g., REST API) available.
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
            
            // Register shutdown hook for proper termination
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