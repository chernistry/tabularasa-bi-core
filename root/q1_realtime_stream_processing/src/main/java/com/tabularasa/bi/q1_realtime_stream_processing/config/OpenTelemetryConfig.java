package com.tabularasa.bi.q1_realtime_stream_processing.config;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Basic OpenTelemetry configuration that provides a {@link Tracer} bean.
 * In production this should be replaced by a fully fledged OTEL SDK setup
 * exporting traces to OTLP/Jaeger. For local development we rely on the
 * global tracer provider (NoOp if no SDK present).
 */
@Configuration
public class OpenTelemetryConfig {

    @Bean
    public Tracer tracer() {
        return GlobalOpenTelemetry.getTracer("q1-realtime-stream");
    }
} 