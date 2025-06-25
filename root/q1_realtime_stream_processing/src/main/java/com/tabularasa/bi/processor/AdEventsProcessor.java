package com.tabularasa.bi.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Simple ad events processor that tracks metrics using Prometheus.
 */
@Component
public final class AdEventsProcessor implements Consumer<String> {

    private static final Logger log = LoggerFactory.getLogger(AdEventsProcessor.class);

    /**
     * Jackson object mapper for JSON processing.
     */
    private final ObjectMapper objectMapper;
    /**
     * Counter for successfully processed events.
     */
    private final Counter processedEventsCounter;
    /**
     * Counter for failed events.
     */
    private final Counter failedEventsCounter;
    /**
     * Timer to measure event processing duration.
     */
    private final Timer eventProcessingTimer;

    /**
     * Constructs a new AdEventsProcessor.
     *
     * @param objectMapper           Jackson object mapper.
     * @param processedEventsCounter Counter for processed events.
     * @param failedEventsCounter    Counter for failed events.
     * @param eventProcessingTimer   Timer for processing duration.
     */
    @Autowired
    public AdEventsProcessor(
            final ObjectMapper objectMapper,
            final Counter processedEventsCounter,
            final Counter failedEventsCounter,
            final Timer eventProcessingTimer) {
        this.objectMapper = objectMapper;
        this.processedEventsCounter = processedEventsCounter;
        this.failedEventsCounter = failedEventsCounter;
        this.eventProcessingTimer = eventProcessingTimer;
    }

    @Override
    public void accept(final String eventJson) {
        final Timer.Sample sample = Timer.start();
        try {
            final JsonNode event = objectMapper.readTree(eventJson);

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

    private void processEvent(final JsonNode event) {
        try {
            // Actual event processing without artificial delays
            // Validation of required fields
            if (!event.has("timestamp")) {
                throw new IllegalArgumentException("Event is missing required 'timestamp' field");
            }
            if (!event.has("campaign_id")) {
                throw new IllegalArgumentException("Event is missing required 'campaign_id' field");
            }
            if (!event.has("event_type")) {
                throw new IllegalArgumentException("Event is missing required 'event_type' field");
            }

            // Logging the event
            log.info("Processed event with campaign_id: {}, event_type: {}",
                    event.path("campaign_id").asText(),
                    event.path("event_type").asText());

            // Place real business logic for event processing here
            // without simulations or random delays

        } catch (Exception e) {
            throw new RuntimeException("Error processing event", e);
        }
    }
}