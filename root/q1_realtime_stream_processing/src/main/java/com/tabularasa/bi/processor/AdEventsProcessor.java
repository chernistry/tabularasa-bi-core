package com.tabularasa.bi.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Simple ad events processor that tracks metrics using Prometheus.
 */
@Slf4j
@Component
public class AdEventsProcessor implements Consumer<String> {

    private final ObjectMapper objectMapper;
    private final Counter processedEventsCounter;
    private final Counter failedEventsCounter;
    private final Timer eventProcessingTimer;

    @Autowired
    public AdEventsProcessor(ObjectMapper objectMapper, 
                             Counter processedEventsCounter,
                             Counter failedEventsCounter,
                             Timer eventProcessingTimer) {
        this.objectMapper = objectMapper;
        this.processedEventsCounter = processedEventsCounter;
        this.failedEventsCounter = failedEventsCounter;
        this.eventProcessingTimer = eventProcessingTimer;
    }

    @Override
    public void accept(String eventJson) {
        Timer.Sample sample = Timer.start();
        try {
            JsonNode event = objectMapper.readTree(eventJson);
            
            // Process the event
            processEvent(event);
            
            // Increment the processed events counter
            processedEventsCounter.increment();
            
            // Record processing time
            sample.stop(eventProcessingTimer);
            
            log.debug("Successfully processed event: {}", eventJson);
        } catch (Exception e) {
            // Increment the failed events counter
            failedEventsCounter.increment();
            
            // Still record the processing time even for failed events
            sample.stop(eventProcessingTimer);
            
            log.error("Failed to process event: {}", eventJson, e);
        }
    }
    
    private void processEvent(JsonNode event) {
        // Simulating event processing delay
        try {
            // In a real application, this would contain the actual event processing logic
            TimeUnit.MILLISECONDS.sleep((long) (Math.random() * 100));
            
            // Example validation check that could throw an exception
            if (!event.has("timestamp")) {
                throw new IllegalArgumentException("Event is missing required 'timestamp' field");
            }
            
            log.info("Processed event with campaign_id: {}, event_type: {}", 
                    event.path("campaign_id").asText("unknown"),
                    event.path("event_type").asText("unknown"));
                    
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Event processing was interrupted", e);
        }
    }
} 