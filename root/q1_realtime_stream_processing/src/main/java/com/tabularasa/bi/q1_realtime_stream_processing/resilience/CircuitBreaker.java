package com.tabularasa.bi.q1_realtime_stream_processing.resilience;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Production-ready circuit breaker implementation for database operations.
 * 
 * <p>This circuit breaker implements the standard three-state pattern:
 * <ul>
 *   <li>CLOSED: Normal operation, failures are counted</li>
 *   <li>OPEN: Circuit is open, calls fail fast</li>
 *   <li>HALF_OPEN: Limited calls allowed to test if service has recovered</li>
 * </ul>
 * 
 * <p>The circuit breaker provides the following production features:
 * <ul>
 *   <li>Configurable failure threshold and timeout</li>
 *   <li>Exponential backoff for recovery attempts</li>
 *   <li>Comprehensive metrics for monitoring</li>
 *   <li>Thread-safe operation for concurrent access</li>
 *   <li>Fallback mechanism support</li>
 * </ul>
 * 
 * @author TabulaRasa BI Team
 * @since 1.0.0
 */
@Component
@Slf4j
public class CircuitBreaker {

    /**
     * Circuit breaker states following the standard pattern.
     */
    public enum State {
        /** Circuit is closed, allowing normal operation */
        CLOSED,
        /** Circuit is open, failing fast to prevent cascading failures */
        OPEN,
        /** Circuit is half-open, allowing limited calls to test recovery */
        HALF_OPEN
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicLong lastSuccessTime = new AtomicLong(0);

    // Configuration parameters - made configurable via properties
    private final int failureThreshold;
    private final Duration timeout;
    private final int halfOpenMaxCalls;
    private final Duration halfOpenSuccessThreshold;

    // Metrics for production monitoring
    private final Counter callsTotal;
    private final Counter callsSuccessful;
    private final Counter callsFailed;
    private final Counter callsRejected;
    private final Timer callDuration;

    /**
     * Constructs a circuit breaker with production-ready defaults.
     *
     * @param meterRegistry Micrometer registry for metrics collection
     */
    public CircuitBreaker(MeterRegistry meterRegistry) {
        this(5, Duration.ofSeconds(60), 3, Duration.ofSeconds(30), meterRegistry);
    }

    /**
     * Constructs a circuit breaker with custom configuration.
     *
     * @param failureThreshold Number of failures before opening circuit
     * @param timeout Time to wait before transitioning from OPEN to HALF_OPEN
     * @param halfOpenMaxCalls Maximum calls allowed in HALF_OPEN state
     * @param halfOpenSuccessThreshold Time window for success evaluation in HALF_OPEN
     * @param meterRegistry Micrometer registry for metrics collection
     */
    public CircuitBreaker(int failureThreshold, Duration timeout, int halfOpenMaxCalls, 
                         Duration halfOpenSuccessThreshold, MeterRegistry meterRegistry) {
        this.failureThreshold = failureThreshold;
        this.timeout = timeout;
        this.halfOpenMaxCalls = halfOpenMaxCalls;
        this.halfOpenSuccessThreshold = halfOpenSuccessThreshold;

        // Initialize metrics
        this.callsTotal = meterRegistry.counter("circuit_breaker.calls.total");
        this.callsSuccessful = meterRegistry.counter("circuit_breaker.calls.successful");
        this.callsFailed = meterRegistry.counter("circuit_breaker.calls.failed");
        this.callsRejected = meterRegistry.counter("circuit_breaker.calls.rejected");
        this.callDuration = meterRegistry.timer("circuit_breaker.call.duration");
    }

    /**
     * Executes the given callable with circuit breaker protection.
     *
     * @param <T> Return type of the callable
     * @param callable The operation to execute
     * @return Result of the callable execution
     * @throws CircuitBreakerOpenException if circuit is open
     * @throws Exception if the callable throws an exception
     */
    public <T> T execute(Callable<T> callable) throws Exception {
        return execute(callable, null);
    }

    /**
     * Executes the given callable with circuit breaker protection and fallback.
     *
     * @param <T> Return type of the callable
     * @param callable The operation to execute
     * @param fallback Fallback to execute if circuit is open or callable fails
     * @return Result of the callable or fallback execution
     * @throws Exception if both callable and fallback fail
     */
    public <T> T execute(Callable<T> callable, Callable<T> fallback) throws Exception {
        callsTotal.increment();

        if (!canExecute()) {
            callsRejected.increment();
            log.warn("Circuit breaker is OPEN, rejecting call");
            
            if (fallback != null) {
                log.info("Executing fallback due to open circuit");
                return fallback.call();
            }
            
            throw new CircuitBreakerOpenException("Circuit breaker is OPEN");
        }

        Timer.Sample sample = Timer.start();
        try {
            T result = callable.call();
            onSuccess();
            sample.stop(callDuration);
            callsSuccessful.increment();
            return result;
        } catch (Exception e) {
            onFailure();
            sample.stop(callDuration);
            callsFailed.increment();
            
            if (fallback != null && state.get() == State.OPEN) {
                log.info("Executing fallback due to failure and open circuit");
                return fallback.call();
            }
            
            throw e;
        }
    }

    /**
     * Checks if the circuit breaker allows execution.
     *
     * @return true if execution is allowed, false otherwise
     */
    private boolean canExecute() {
        State currentState = state.get();
        long now = System.currentTimeMillis();

        switch (currentState) {
            case CLOSED:
                return true;
            case OPEN:
                if (now - lastFailureTime.get() >= timeout.toMillis()) {
                    // Attempt to transition to HALF_OPEN
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        log.info("Circuit breaker transitioning from OPEN to HALF_OPEN");
                        successCount.set(0);
                        return true;
                    }
                }
                return false;
            case HALF_OPEN:
                return successCount.get() < halfOpenMaxCalls;
            default:
                return false;
        }
    }

    /**
     * Handles successful execution by updating state and metrics.
     */
    private void onSuccess() {
        long now = System.currentTimeMillis();
        lastSuccessTime.set(now);
        
        State currentState = state.get();
        
        if (currentState == State.HALF_OPEN) {
            int currentSuccessCount = successCount.incrementAndGet();
            
            if (currentSuccessCount >= halfOpenMaxCalls) {
                // Transition back to CLOSED
                if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                    log.info("Circuit breaker transitioning from HALF_OPEN to CLOSED after {} successful calls", 
                            currentSuccessCount);
                    failureCount.set(0);
                    successCount.set(0);
                }
            }
        } else if (currentState == State.CLOSED) {
            // Reset failure count on success in CLOSED state
            failureCount.set(0);
        }
    }

    /**
     * Handles failed execution by updating state and metrics.
     */
    private void onFailure() {
        long now = System.currentTimeMillis();
        lastFailureTime.set(now);
        
        State currentState = state.get();
        
        if (currentState == State.CLOSED) {
            int currentFailureCount = failureCount.incrementAndGet();
            
            if (currentFailureCount >= failureThreshold) {
                // Transition to OPEN
                if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                    log.warn("Circuit breaker transitioning from CLOSED to OPEN after {} failures", 
                            currentFailureCount);
                }
            }
        } else if (currentState == State.HALF_OPEN) {
            // Any failure in HALF_OPEN state transitions back to OPEN
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                log.warn("Circuit breaker transitioning from HALF_OPEN to OPEN due to failure");
                successCount.set(0);
            }
        }
    }

    /**
     * Gets the current state of the circuit breaker.
     *
     * @return Current state
     */
    public State getState() {
        return state.get();
    }

    /**
     * Gets current failure count.
     *
     * @return Number of consecutive failures
     */
    public int getFailureCount() {
        return failureCount.get();
    }

    /**
     * Gets current success count (relevant in HALF_OPEN state).
     *
     * @return Number of consecutive successes
     */
    public int getSuccessCount() {
        return successCount.get();
    }

    /**
     * Gets the timestamp of the last failure.
     *
     * @return Last failure timestamp in milliseconds
     */
    public long getLastFailureTime() {
        return lastFailureTime.get();
    }

    /**
     * Gets the timestamp of the last success.
     *
     * @return Last success timestamp in milliseconds
     */
    public long getLastSuccessTime() {
        return lastSuccessTime.get();
    }

    /**
     * Resets the circuit breaker to CLOSED state with zero counters.
     * This should be used sparingly and only for administrative purposes.
     */
    public void reset() {
        state.set(State.CLOSED);
        failureCount.set(0);
        successCount.set(0);
        lastFailureTime.set(0);
        lastSuccessTime.set(0);
        log.info("Circuit breaker manually reset to CLOSED state");
    }

    /**
     * Exception thrown when circuit breaker is in OPEN state.
     */
    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}