package com.tabularasa.bi.q1_realtime_stream_processing.health;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuator.health.Health;
import org.springframework.boot.actuator.health.HealthIndicator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

/**
 * Production-ready health indicator for database connectivity and performance monitoring.
 * 
 * <p>This health indicator provides comprehensive database health checks including:
 * <ul>
 *   <li>Connection availability and response time</li>
 *   <li>Query execution performance</li>
 *   <li>Connection pool health metrics</li>
 *   <li>Database schema validation</li>
 * </ul>
 * 
 * <p>The health check performs both basic connectivity tests and application-specific
 * validations to ensure the database is ready for production workloads.
 * 
 * @author TabulaRasa BI Team
 * @version 1.0.0
 * @since 1.0.0
 */
@Component
@Slf4j
public class DatabaseHealthIndicator implements HealthIndicator {

    private final DataSource dataSource;
    
    // Health check configuration
    private static final String TEST_QUERY = "SELECT 1 as health_check, NOW() as current_time, version() as db_version";
    private static final String TABLE_CHECK_QUERY = 
        "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_name = 'aggregated_campaign_stats'";
    private static final Duration HEALTH_CHECK_TIMEOUT = Duration.ofSeconds(5);
    private static final long RESPONSE_TIME_WARNING_THRESHOLD_MS = 1000; // 1 second

    /**
     * Constructs a new database health indicator.
     *
     * @param dataSource The DataSource to monitor for health
     */
    @Autowired
    public DatabaseHealthIndicator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Performs comprehensive database health check including connectivity and performance validation.
     * 
     * <p>This method executes multiple health check operations:
     * <ul>
     *   <li>Basic connectivity test with timeout</li>
     *   <li>Query execution time measurement</li>
     *   <li>Schema validation checks</li>
     *   <li>Connection pool status evaluation</li>
     * </ul>
     * 
     * <p>The health status is determined based on:
     * <ul>
     *   <li>Connection success/failure</li>
     *   <li>Query response time thresholds</li>
     *   <li>Schema integrity validation</li>
     *   <li>Connection pool availability</li>
     * </ul>
     * 
     * @return Health status with detailed diagnostic information
     */
    @Override
    public Health health() {
        Instant startTime = Instant.now();
        
        try {
            return performHealthCheck(startTime);
        } catch (Exception e) {
            log.error("Database health check failed with unexpected error", e);
            return Health.down()
                    .withDetail("error", "Unexpected health check failure")
                    .withDetail("exception", e.getClass().getSimpleName())
                    .withDetail("message", e.getMessage())
                    .withDetail("timestamp", startTime)
                    .build();
        }
    }

    /**
     * Executes the detailed health check operations with comprehensive error handling.
     *
     * @param startTime The start time of the health check for performance measurement
     * @return Health status with detailed metrics and diagnostic information
     */
    private Health performHealthCheck(Instant startTime) {
        try (Connection connection = dataSource.getConnection()) {
            // Set query timeout to prevent hanging health checks
            connection.setNetworkTimeout(null, (int) HEALTH_CHECK_TIMEOUT.toMillis());
            
            Health.Builder healthBuilder = Health.up();
            
            // Perform basic connectivity test
            DatabaseHealthMetrics metrics = executeHealthCheckQuery(connection, startTime);
            
            // Add performance metrics
            healthBuilder
                    .withDetail("responseTime", metrics.responseTimeMs + "ms")
                    .withDetail("performanceStatus", determinePerformanceStatus(metrics.responseTimeMs))
                    .withDetail("dbVersion", metrics.databaseVersion)
                    .withDetail("connectionTime", metrics.connectionTimeMs + "ms")
                    .withDetail("timestamp", startTime);

            // Perform schema validation
            boolean schemaValid = validateDatabaseSchema(connection);
            healthBuilder
                    .withDetail("schemaValid", schemaValid)
                    .withDetail("requiredTablesPresent", schemaValid);

            // Add connection pool information if available
            addConnectionPoolMetrics(healthBuilder);

            // Determine overall health status
            if (!schemaValid) {
                return healthBuilder
                        .status("DOWN")
                        .withDetail("reason", "Database schema validation failed")
                        .build();
            }

            if (metrics.responseTimeMs > RESPONSE_TIME_WARNING_THRESHOLD_MS) {
                return healthBuilder
                        .status("WARNING")
                        .withDetail("reason", "Database response time exceeds warning threshold")
                        .build();
            }

            return healthBuilder.build();

        } catch (SQLException e) {
            log.warn("Database health check failed due to SQL error", e);
            return Health.down()
                    .withDetail("error", "Database connection or query failed")
                    .withDetail("sqlState", e.getSQLState())
                    .withDetail("errorCode", e.getErrorCode())
                    .withDetail("message", e.getMessage())
                    .withDetail("timestamp", startTime)
                    .build();
        } catch (Exception e) {
            log.error("Database health check failed with unexpected error", e);
            return Health.down()
                    .withDetail("error", "Unexpected database health check failure")
                    .withDetail("exception", e.getClass().getSimpleName())
                    .withDetail("message", e.getMessage())
                    .withDetail("timestamp", startTime)
                    .build();
        }
    }

    /**
     * Executes the health check query and measures performance metrics.
     *
     * @param connection Database connection to use for the health check
     * @param startTime Start time for connection timing measurement
     * @return DatabaseHealthMetrics containing performance and connectivity data
     * @throws SQLException if query execution fails
     */
    private DatabaseHealthMetrics executeHealthCheckQuery(Connection connection, Instant startTime) throws SQLException {
        Instant queryStartTime = Instant.now();
        long connectionTimeMs = Duration.between(startTime, queryStartTime).toMillis();
        
        try (PreparedStatement statement = connection.prepareStatement(TEST_QUERY);
             ResultSet resultSet = statement.executeQuery()) {
            
            if (resultSet.next()) {
                Instant queryEndTime = Instant.now();
                long responseTimeMs = Duration.between(queryStartTime, queryEndTime).toMillis();
                String databaseVersion = resultSet.getString("db_version");
                
                log.debug("Database health check successful - Response time: {}ms, Version: {}", 
                         responseTimeMs, databaseVersion);
                
                return new DatabaseHealthMetrics(responseTimeMs, connectionTimeMs, databaseVersion);
            } else {
                throw new SQLException("Health check query returned no results");
            }
        }
    }

    /**
     * Validates that required database schema elements are present and accessible.
     *
     * @param connection Database connection to use for schema validation
     * @return true if schema validation passes, false otherwise
     */
    private boolean validateDatabaseSchema(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(TABLE_CHECK_QUERY);
             ResultSet resultSet = statement.executeQuery()) {
            
            if (resultSet.next()) {
                int tableCount = resultSet.getInt("table_count");
                boolean schemaValid = tableCount > 0;
                
                if (!schemaValid) {
                    log.warn("Database schema validation failed - Required table 'aggregated_campaign_stats' not found");
                }
                
                return schemaValid;
            }
            return false;
        } catch (SQLException e) {
            log.warn("Schema validation query failed", e);
            return false;
        }
    }

    /**
     * Adds connection pool metrics to the health check if available.
     *
     * @param healthBuilder Health builder to add metrics to
     */
    private void addConnectionPoolMetrics(Health.Builder healthBuilder) {
        try {
            // Attempt to get HikariCP metrics if available
            if (dataSource instanceof com.zaxxer.hikari.HikariDataSource) {
                com.zaxxer.hikari.HikariDataSource hikariDataSource = 
                    (com.zaxxer.hikari.HikariDataSource) dataSource;
                
                com.zaxxer.hikari.HikariPoolMXBean poolBean = hikariDataSource.getHikariPoolMXBean();
                if (poolBean != null) {
                    healthBuilder
                            .withDetail("pool.active", poolBean.getActiveConnections())
                            .withDetail("pool.idle", poolBean.getIdleConnections())
                            .withDetail("pool.total", poolBean.getTotalConnections())
                            .withDetail("pool.awaiting", poolBean.getThreadsAwaitingConnection())
                            .withDetail("pool.max", hikariDataSource.getMaximumPoolSize());
                }
            }
        } catch (Exception e) {
            log.debug("Unable to retrieve connection pool metrics", e);
            // Continue without pool metrics - not critical for health check
        }
    }

    /**
     * Determines performance status based on response time thresholds.
     *
     * @param responseTimeMs Query response time in milliseconds
     * @return Performance status string
     */
    private String determinePerformanceStatus(long responseTimeMs) {
        if (responseTimeMs < 100) {
            return "EXCELLENT";
        } else if (responseTimeMs < 500) {
            return "GOOD";
        } else if (responseTimeMs < RESPONSE_TIME_WARNING_THRESHOLD_MS) {
            return "ACCEPTABLE";
        } else {
            return "SLOW";
        }
    }

    /**
     * Container class for database health metrics.
     */
    private static class DatabaseHealthMetrics {
        final long responseTimeMs;
        final long connectionTimeMs;
        final String databaseVersion;

        DatabaseHealthMetrics(long responseTimeMs, long connectionTimeMs, String databaseVersion) {
            this.responseTimeMs = responseTimeMs;
            this.connectionTimeMs = connectionTimeMs;
            this.databaseVersion = databaseVersion;
        }
    }
}