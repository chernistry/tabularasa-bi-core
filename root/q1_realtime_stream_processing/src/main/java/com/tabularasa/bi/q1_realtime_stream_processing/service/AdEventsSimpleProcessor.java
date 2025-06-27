package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.tabularasa.bi.q1_realtime_stream_processing.model.AdEvent;
import com.tabularasa.bi.q1_realtime_stream_processing.model.AggregatedCampaignStats;
import com.tabularasa.bi.q1_realtime_stream_processing.repository.AggregatedCampaignStatsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple processor for ad events that uses Kafka listeners and in-memory buffers.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Profile("simple")
public class AdEventsSimpleProcessor {

    private final AggregatedCampaignStatsRepository repository;
    
    // In-memory buffers for aggregation
    private final Map<String, Map<String, Long>> eventCounts = new ConcurrentHashMap<>();
    private final Map<String, Map<String, BigDecimal>> bidAmounts = new ConcurrentHashMap<>();
    
    // Time window tracking
    private LocalDateTime windowStartTime = LocalDateTime.now();
    private final AtomicBoolean processing = new AtomicBoolean(false);

    /**
     * Start processing events from Kafka.
     */
    public void startProcessing() {
        log.info("Starting simple ad event processing");
        processing.set(true);
        windowStartTime = LocalDateTime.now();
    }

    /**
     * Stop processing events from Kafka.
     */
    public void stopProcessing() {
        log.info("Stopping simple ad event processing");
        processing.set(false);
        // Flush any remaining data
        flushAggregatedData();
    }

    /**
     * Kafka listener for ad events.
     * 
     * @param adEvent The ad event from Kafka
     */
    @KafkaListener(topics = "${app.kafka.topics.ad-events}", groupId = "${app.kafka.group-id}")
    public void processEvent(AdEvent adEvent) {
        if (!processing.get()) {
            return; // Skip if not processing
        }
        
        try {
            log.debug("Processing ad event: {}", adEvent);
            
            String campaignId = adEvent.getCampaignId();
            String eventType = adEvent.getEventType();
            
            // Update event counts
            eventCounts.computeIfAbsent(campaignId, k -> new ConcurrentHashMap<>())
                    .compute(eventType, (k, v) -> (v == null) ? 1 : v + 1);
            
            // Update bid amounts
            bidAmounts.computeIfAbsent(campaignId, k -> new ConcurrentHashMap<>())
                    .compute(eventType, (k, v) -> {
                        if (v == null) {
                            return adEvent.getBidAmountUsd();
                        } else {
                            return v.add(adEvent.getBidAmountUsd());
                        }
                    });
        } catch (Exception e) {
            log.error("Error processing ad event: {}", e.getMessage(), e);
        }
    }

    /**
     * Scheduled method to flush aggregated data to the database.
     * Runs every minute.
     */
    @Scheduled(fixedRate = 60000) // Every minute
    public void flushAggregatedData() {
        if (!processing.get() && eventCounts.isEmpty()) {
            return; // Skip if not processing and no data
        }
        
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime windowEndTime = now;
        
        log.info("Flushing aggregated data from {} to {}", windowStartTime, windowEndTime);
        
        // Process each campaign's data
        for (Map.Entry<String, Map<String, Long>> campaignEntry : eventCounts.entrySet()) {
            String campaignId = campaignEntry.getKey();
            Map<String, Long> counts = campaignEntry.getValue();
            
            // Process each event type for this campaign
            for (Map.Entry<String, Long> eventEntry : counts.entrySet()) {
                String eventType = eventEntry.getKey();
                Long count = eventEntry.getValue();
                
                // Get the bid amount for this campaign and event type
                BigDecimal totalBid = bidAmounts.getOrDefault(campaignId, new HashMap<>())
                        .getOrDefault(eventType, BigDecimal.ZERO);
                
                // Check if we already have a record for this window
                Optional<AggregatedCampaignStats> existingStats = repository
                        .findByCampaignIdAndEventTypeAndWindowStartTimeAndWindowEndTime(
                                campaignId, eventType, windowStartTime, windowEndTime);
                
                if (existingStats.isPresent()) {
                    // Update existing record
                    AggregatedCampaignStats stats = existingStats.get();
                    stats.setEventCount(stats.getEventCount() + count);
                    stats.setTotalBidAmount(stats.getTotalBidAmount().add(totalBid));
                    stats.setLastUpdated(now);
                    repository.save(stats);
                } else {
                    // Create new record
                    AggregatedCampaignStats stats = AggregatedCampaignStats.builder()
                            .campaignId(campaignId)
                            .eventType(eventType)
                            .windowStartTime(windowStartTime)
                            .windowEndTime(windowEndTime)
                            .eventCount(count)
                            .totalBidAmount(totalBid)
                            .lastUpdated(now)
                            .build();
                    repository.save(stats);
                }
            }
        }
        
        // Clear the buffers for the next window
        eventCounts.clear();
        bidAmounts.clear();
        
        // Set the new window start time
        windowStartTime = now;
        
        log.info("Aggregated data flushed successfully, starting new window");
    }
}