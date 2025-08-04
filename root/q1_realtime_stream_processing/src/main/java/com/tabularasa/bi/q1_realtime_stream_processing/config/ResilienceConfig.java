package com.tabularasa.bi.q1_realtime_stream_processing.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Production-grade resilience configuration using Resilience4j.
 * Provides circuit breakers, retries, and bulkheads for fault tolerance.
 */
@Configuration
@ConditionalOnProperty(name = "tabularasa.features.circuit-breaker-enabled", havingValue = "true", matchIfMissing = true)
public class ResilienceConfig {

    /**
     * Configure circuit breaker for database operations.
     */
    @Bean
    public CircuitBreaker databaseCircuitBreaker(CircuitBreakerRegistry registry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(50.0f)
                .slowCallRateThreshold(50.0f)
                .slowCallDurationThreshold(Duration.ofSeconds(2))
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(SQLException.class, CannotGetJdbcConnectionException.class)
                .ignoreExceptions(IllegalArgumentException.class)
                .build();
        
        return registry.circuitBreaker("database", config);
    }

    /**
     * Configure circuit breaker for Kafka operations.
     */
    @Bean
    public CircuitBreaker kafkaCircuitBreaker(CircuitBreakerRegistry registry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
                .slidingWindowSize(60) // 60 seconds
                .minimumNumberOfCalls(5)
                .failureRateThreshold(60.0f)
                .slowCallRateThreshold(60.0f)
                .slowCallDurationThreshold(Duration.ofSeconds(5))
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordExceptions(IOException.class, TimeoutException.class)
                .build();
        
        return registry.circuitBreaker("kafka", config);
    }

    /**
     * Configure circuit breaker for external API calls.
     */
    @Bean
    public CircuitBreaker apiCircuitBreaker(CircuitBreakerRegistry registry) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(50)
                .minimumNumberOfCalls(10)
                .failureRateThreshold(70.0f)
                .slowCallRateThreshold(70.0f)
                .slowCallDurationThreshold(Duration.ofSeconds(3))
                .waitDurationInOpenState(Duration.ofSeconds(45))
                .permittedNumberOfCallsInHalfOpenState(5)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        
        return registry.circuitBreaker("external-api", config);
    }

    /**
     * Configure retry mechanism for transient failures.
     */
    @Bean
    public Retry databaseRetry(RetryRegistry registry) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .intervalFunction(attempt -> Duration.ofMillis(500 * attempt).toMillis())
                .retryExceptions(SQLException.class, CannotGetJdbcConnectionException.class)
                .ignoreExceptions(IllegalArgumentException.class)
                .failAfterMaxAttempts(true)
                .build();
        
        return registry.retry("database", config);
    }

    /**
     * Configure retry mechanism for Kafka operations.
     */
    @Bean
    public Retry kafkaRetry(RetryRegistry registry) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(5)
                .waitDuration(Duration.ofSeconds(1))
                .intervalFunction(attempt -> Duration.ofSeconds((long) Math.pow(2, attempt - 1)).toMillis())
                .retryExceptions(IOException.class, TimeoutException.class)
                .failAfterMaxAttempts(true)
                .build();
        
        return registry.retry("kafka", config);
    }

    /**
     * Register circuit breaker metrics with Micrometer.
     */
    @Bean
    public TaggedCircuitBreakerMetrics taggedCircuitBreakerMetrics(
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry) {
        TaggedCircuitBreakerMetrics metrics = TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }

    /**
     * Register retry metrics with Micrometer.
     */
    @Bean
    public TaggedRetryMetrics taggedRetryMetrics(
            RetryRegistry retryRegistry,
            MeterRegistry meterRegistry) {
        TaggedRetryMetrics metrics = TaggedRetryMetrics.ofRetryRegistry(retryRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }
}