package com.tabularasa.bi.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for custom application metrics using Micrometer.
 */
@Configuration
public class MetricsConfig {

    private static final double P50 = 0.5;
    private static final double P95 = 0.95;
    private static final double P99 = 0.99;

    /**
     * Micrometer registry for metrics.
     */
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new MetricsConfig.
     *
     * @param meterRegistry The meter registry.
     */
    @Autowired
    public MetricsConfig(final MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Counter for tracking processed events.
     *
     * @return A counter for processed events.
     */
    @Bean
    public Counter processedEventsCounter() {
        return Counter.builder("app.events.processed")
                .description("The number of events processed")
                .tag("type", "ad_event")
                .register(meterRegistry);
    }

    /**
     * Counter for tracking failed events.
     *
     * @return A counter for failed events.
     */
    @Bean
    public Counter failedEventsCounter() {
        return Counter.builder("app.events.failed")
                .description("The number of events that failed processing")
                .tag("type", "ad_event")
                .register(meterRegistry);
    }

    /**
     * Timer for measuring event processing duration.
     *
     * @return A timer for event processing.
     */
    @Bean
    public Timer eventProcessingTimer() {
        return Timer.builder("app.events.processing.time")
                .description("Time taken to process events")
                .tag("type", "ad_event")
                .publishPercentiles(P50, P95, P99)
                .register(meterRegistry);
    }
}