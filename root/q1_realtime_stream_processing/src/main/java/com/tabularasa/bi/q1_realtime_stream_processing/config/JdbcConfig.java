package com.tabularasa.bi.q1_realtime_stream_processing.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * JDBC configuration for direct database access in Spark profile.
 * Since we disabled JPA auto-configuration, we need to provide these beans manually.
 */
@Configuration
@Profile("spark")
public class JdbcConfig {

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUser;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    @Value("${spring.datasource.driver-class-name:org.postgresql.Driver}")
    private String driverClassName;

    @Bean
    @Primary
    public DataSource dataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(dbUrl);
        ds.setUsername(dbUser);
        ds.setPassword(dbPassword);
        ds.setDriverClassName(driverClassName);
        ds.setPoolName("SparkStreamProcessor");
        
        // Configure connection pool specifically for Spark workloads
        ds.setMaximumPoolSize(10);
        ds.setMinimumIdle(2);
        ds.setIdleTimeout(60000);
        ds.setConnectionTimeout(30000);
        ds.setMaxLifetime(1800000);
        ds.setAutoCommit(false);
        
        // Enable validation
        ds.setConnectionTestQuery("SELECT 1");
        ds.setValidationTimeout(5000);
        
        // Enable leakDetectionThreshold
        ds.setLeakDetectionThreshold(30000);
        
        return ds;
    }

    @Bean
    @Primary
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setQueryTimeout(30); // 30 seconds timeout
        return jdbcTemplate;
    }
} 