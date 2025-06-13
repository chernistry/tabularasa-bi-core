package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service for processing ad event streams without Apache Spark.
 * Aggregates events by time window, campaign, and event type, then saves to DB.
 */
@Service
public class AdEventsSimpleProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdEventsSimpleProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final JdbcTemplate jdbcTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Map<WindowKey, AggregatedStats> aggregatedDataMap = new ConcurrentHashMap<>();

    @Value("${spring.profiles.active:default}")
    private String activeProfile;

    @Autowired
    public AdEventsSimpleProcessor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        // Start periodic DB save
        scheduler.scheduleAtFixedRate(this::saveAggregatedStatsToDb, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * Kafka listener for ad events
     */
    @KafkaListener(topics = "${kafka.topic.ad-events}", groupId = "${kafka.group.id}")
    public void processAdEvent(String adEventJson) {
        try {
            // Log the raw JSON received
            logger.debug("Received raw JSON from Kafka: {}", adEventJson);

            JsonNode eventNode = objectMapper.readTree(adEventJson);

            // Log parsed fields from JSON
            logger.debug("Parsing fields from JSON");

            // Robust timestamp parsing to handle 'Z' (UTC)
            String timestampStr = eventNode.get("timestamp").asText();
            java.time.Instant instant = java.time.Instant.parse(timestampStr); // Handles 'Z' for UTC
            LocalDateTime timestamp = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
            logger.debug("Parsed timestamp: {}", timestamp);

            String campaignId = eventNode.get("campaign_id").asText();
            logger.debug("Parsed campaignId: {}", campaignId);

            String eventType = eventNode.get("event_type").asText();
            logger.debug("Parsed eventType: {}", eventType);

            double bidAmount = eventNode.get("bid_amount_usd").asDouble();
            logger.debug("Parsed bidAmount: {}", bidAmount);

            // Calculate window start (truncate to minute)
            LocalDateTime windowStart = timestamp.withSecond(0).withNano(0);
            logger.debug("Calculated windowStart: {}", windowStart);

            // Key for aggregation: (campaign_id, event_type, window_start)
            WindowKey key = new WindowKey(campaignId, eventType, windowStart);
            logger.debug("Created WindowKey: {}", key);

            // Update aggregation
            aggregatedDataMap.compute(key, (k, stats) -> {
                if (stats == null) {
                    stats = new AggregatedStats();
                    logger.debug("Created new AggregatedStats for key: {}", k);
                } else {
                    logger.debug("Updating existing AggregatedStats for key: {}", k);
                }
                stats.eventCount++;
                stats.totalBidAmount += bidAmount;
                logger.debug("Updated stats: count={}, totalBid={}", stats.eventCount, stats.totalBidAmount);
                return stats;
            });

            logger.debug("Processed event: campaign={}, type={}, window_start={}", campaignId, eventType, windowStart);
            logger.debug("Current aggregatedDataMap size: {}", aggregatedDataMap.size());
        } catch (Exception e) {
            logger.error("Error processing ad event: {}", adEventJson, e);
        }
    }

    /**
     * Periodically saves aggregated data to DB
     */
    @Transactional
    public void saveAggregatedStatsToDb() {
        logger.debug("Saving aggregated data, record count: {}", aggregatedDataMap.size());
        if (aggregatedDataMap.isEmpty()) {
            return;
        }
        // Copy and clear map
        Map<WindowKey, AggregatedStats> dataToSave = new HashMap<>(aggregatedDataMap);
        aggregatedDataMap.clear();
        // SQL for upsert
        String sql = "INSERT INTO aggregated_campaign_stats " +
                "(campaign_id, event_type, window_start_time, event_count, total_bid_amount, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (campaign_id, event_type, window_start_time) DO UPDATE SET " +
                "event_count = aggregated_campaign_stats.event_count + EXCLUDED.event_count, " +
                "total_bid_amount = aggregated_campaign_stats.total_bid_amount + EXCLUDED.total_bid_amount, " +
                "updated_at = CURRENT_TIMESTAMP";
        for (Map.Entry<WindowKey, AggregatedStats> entry : dataToSave.entrySet()) {
            WindowKey key = entry.getKey();
            AggregatedStats stats = entry.getValue();
            try {
                jdbcTemplate.update(sql,
                        key.campaignId,
                        key.eventType,
                        key.windowStart,
                        stats.eventCount,
                        stats.totalBidAmount);
                logger.debug("Saved aggregated stats: campaign={}, type={}, window_start={}, count={}, total_bid_amount={}",
                        key.campaignId, key.eventType, key.windowStart, stats.eventCount, stats.totalBidAmount);
            } catch (Exception e) {
                logger.error("Error saving aggregated stats: {}", key, e);
            }
        }
    }

    /**
     * Key for identifying aggregation window
     */
    private static class WindowKey {
        private final String campaignId;
        private final String eventType;
        private final LocalDateTime windowStart;
        public WindowKey(String campaignId, String eventType, LocalDateTime windowStart) {
            this.campaignId = campaignId;
            this.eventType = eventType;
            this.windowStart = windowStart;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WindowKey windowKey = (WindowKey) o;
            return campaignId.equals(windowKey.campaignId) &&
                    eventType.equals(windowKey.eventType) &&
                    windowStart.equals(windowKey.windowStart);
        }
        @Override
        public int hashCode() {
            int result = campaignId.hashCode();
            result = 31 * result + eventType.hashCode();
            result = 31 * result + windowStart.hashCode();
            return result;
        }
        @Override
        public String toString() {
            return "WindowKey{" +
                    "campaignId='" + campaignId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", windowStart=" + windowStart +
                    '}';
        }
    }

    /**
     * Aggregated stats for a window
     */
    private static class AggregatedStats {
        private long eventCount;
        private double totalBidAmount;
        @Override
        public String toString() {
            return "AggregatedStats{" +
                    "eventCount=" + eventCount +
                    ", totalBidAmount=" + totalBidAmount +
                    '}';
        }
    }
}