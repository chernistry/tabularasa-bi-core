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

    private final MeterRegistry meterRegistry;

    @Autowired
    public MetricsConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Counter for tracking processed events.
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
     */
    @Bean
    public Timer eventProcessingTimer() {
        return Timer.builder("app.events.processing.time")
                .description("Time taken to process events")
                .tag("type", "ad_event")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }
} 