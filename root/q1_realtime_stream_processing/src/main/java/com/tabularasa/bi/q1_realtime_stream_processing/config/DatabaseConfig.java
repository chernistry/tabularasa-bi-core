package com.tabularasa.bi.q1_realtime_stream_processing.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Database configuration for schema initialization and connection check.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class DatabaseConfig {

    private final DataSource dataSource;
    private final ReentrantLock dbInitLock = new ReentrantLock();

    @Value("${spring.datasource.initialization-mode:never}")
    private String initMode;

    /**
     * Initializes the database using the schema.sql SQL script.
     * @return DataSourceInitializer for database schema initialization
     */
    @Bean
    public DataSourceInitializer dataSourceInitializer() {
        ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator();
        
        try {
            resourceDatabasePopulator.addScript(new ClassPathResource("schema.sql"));
            resourceDatabasePopulator.setSeparator(";");
            resourceDatabasePopulator.setContinueOnError(true);
        } catch (Exception e) {
            log.error("Error setting up database initializer", e);
        }
        
        DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
        dataSourceInitializer.setDataSource(dataSource);
        dataSourceInitializer.setDatabasePopulator(resourceDatabasePopulator);
        
        return dataSourceInitializer;
    }

    /**
     * Checks the database connection at application startup.
     */
    @PostConstruct
    public void checkDatabaseConnection() {
        dbInitLock.lock();
        try {
            log.info("Checking database connection...");
            
            try (Connection connection = dataSource.getConnection()) {
                // Check connection and table existence
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeQuery("SELECT 1");
                }
                log.info("Database connection successful");
                
                // Check for the view that may cause an error
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'v_aggregated_campaign_stats')");
                    log.info("View check successful");
                } catch (SQLException e) {
                    log.warn("Could not check view existence: {}", e.getMessage());
                }
                
            } catch (SQLException e) {
                log.error("Database connection failed: {}", e.getMessage());
                // Do not interrupt application startup, just log the error
            }
        } finally {
            dbInitLock.unlock();
        }
    }
} 