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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Класс для обработки потока событий рекламы без использования Apache Spark
 * Агрегирует события по временным окнам и кампаниям, а затем сохраняет их в БД
 */
@Service
public class AdEventsSimpleProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AdEventsSimpleProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(30);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    private final JdbcTemplate jdbcTemplate;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Map<WindowKey, AggregatedStats> aggregatedDataMap = new ConcurrentHashMap<>();

    @Value("${spring.profiles.active:default}")
    private String activeProfile;

    @Autowired
    public AdEventsSimpleProcessor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        
        // Запускаем периодическое сохранение агрегированных данных
        scheduler.scheduleAtFixedRate(this::saveAggregatedStatsToDb, 30, 30, TimeUnit.SECONDS);
    }

    /**
     * Слушатель Kafka для обработки событий рекламы
     */
    @KafkaListener(topics = "${kafka.topic.ad-events}", groupId = "${kafka.group.id}")
    public void processAdEvent(String adEventJson) {
        try {
            JsonNode eventNode = objectMapper.readTree(adEventJson);
            
            LocalDateTime timestamp = LocalDateTime.parse(
                    eventNode.get("timestamp").asText(), 
                    ISO_FORMATTER);
            
            long campaignId = eventNode.get("campaign_id").asLong();
            String eventType = eventNode.get("event_type").asText();
            double bidAmount = eventNode.get("bid_amount_usd").asDouble();
            
            // Определяем окно для события
            LocalDateTime windowStart = timestamp.withNano(0)
                    .withSecond((int)(timestamp.getSecond() / WINDOW_SIZE.getSeconds() * WINDOW_SIZE.getSeconds()));
            LocalDateTime windowEnd = windowStart.plus(WINDOW_SIZE);
            
            // Ключ для доступа к агрегированным данным
            WindowKey key = new WindowKey(campaignId, windowStart, windowEnd);
            
            // Обновляем агрегированные данные
            aggregatedDataMap.compute(key, (k, stats) -> {
                if (stats == null) {
                    stats = new AggregatedStats();
                }
                
                // Инкрементируем счетчики в зависимости от типа события
                if ("impression".equals(eventType)) {
                    stats.impressions++;
                } else if ("click".equals(eventType)) {
                    stats.clicks++;
                } else if ("conversion".equals(eventType)) {
                    stats.conversions++;
                }
                
                stats.totalBidAmountUsd += bidAmount;
                return stats;
            });
            
            logger.debug("Обработано событие: campaign={}, type={}, window={}->{}",
                    campaignId, eventType, windowStart, windowEnd);
            
        } catch (Exception e) {
            logger.error("Ошибка при обработке события рекламы: {}", adEventJson, e);
        }
    }
    
    /**
     * Периодически сохраняет агрегированные данные в БД
     */
    @Transactional
    public void saveAggregatedStatsToDb() {
        logger.debug("Запуск сохранения агрегированных данных, количество записей: {}", aggregatedDataMap.size());
        
        if (aggregatedDataMap.isEmpty()) {
            return;
        }
        
        // Создаем локальную копию и очищаем оригинальную карту
        Map<WindowKey, AggregatedStats> dataToSave = new HashMap<>(aggregatedDataMap);
        aggregatedDataMap.clear();
        
        // SQL для вставки/обновления данных
        String sql = "INSERT INTO aggregated_campaign_stats " +
                     "(campaign_id, window_start, window_end, impressions, clicks, conversions, total_bid_amount_usd, updated_at) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                     "ON CONFLICT (campaign_id, window_start, window_end) DO UPDATE SET " +
                     "impressions = aggregated_campaign_stats.impressions + EXCLUDED.impressions, " +
                     "clicks = aggregated_campaign_stats.clicks + EXCLUDED.clicks, " +
                     "conversions = aggregated_campaign_stats.conversions + EXCLUDED.conversions, " +
                     "total_bid_amount_usd = aggregated_campaign_stats.total_bid_amount_usd + EXCLUDED.total_bid_amount_usd, " +
                     "updated_at = CURRENT_TIMESTAMP";
        
        // Обработка записей в пакетном режиме
        for (Map.Entry<WindowKey, AggregatedStats> entry : dataToSave.entrySet()) {
            WindowKey key = entry.getKey();
            AggregatedStats stats = entry.getValue();
            
            try {
                jdbcTemplate.update(sql,
                        key.campaignId,
                        key.windowStart,
                        key.windowEnd,
                        stats.impressions,
                        stats.clicks,
                        stats.conversions,
                        stats.totalBidAmountUsd);
                
                logger.debug("Сохранена агрегированная статистика: campaign={}, window={}->{}, impressions={}, clicks={}, conversions={}",
                        key.campaignId, key.windowStart, key.windowEnd, stats.impressions, stats.clicks, stats.conversions);
            } catch (Exception e) {
                logger.error("Ошибка при сохранении агрегированной статистики: {}", key, e);
            }
        }
    }
    
    /**
     * Ключ для идентификации временного окна и кампании
     */
    private static class WindowKey {
        private final long campaignId;
        private final LocalDateTime windowStart;
        private final LocalDateTime windowEnd;
        
        public WindowKey(long campaignId, LocalDateTime windowStart, LocalDateTime windowEnd) {
            this.campaignId = campaignId;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WindowKey windowKey = (WindowKey) o;
            return campaignId == windowKey.campaignId && 
                   windowStart.equals(windowKey.windowStart) && 
                   windowEnd.equals(windowKey.windowEnd);
        }
        
        @Override
        public int hashCode() {
            int result = (int) (campaignId ^ (campaignId >>> 32));
            result = 31 * result + windowStart.hashCode();
            result = 31 * result + windowEnd.hashCode();
            return result;
        }
        
        @Override
        public String toString() {
            return "WindowKey{" +
                   "campaignId=" + campaignId +
                   ", windowStart=" + windowStart +
                   ", windowEnd=" + windowEnd +
                   '}';
        }
    }
    
    /**
     * Класс для хранения агрегированных данных по событиям
     */
    private static class AggregatedStats {
        private long impressions;
        private long clicks;
        private long conversions;
        private double totalBidAmountUsd;
        
        @Override
        public String toString() {
            return "AggregatedStats{" +
                   "impressions=" + impressions +
                   ", clicks=" + clicks +
                   ", conversions=" + conversions +
                   ", totalBidAmountUsd=" + totalBidAmountUsd +
                   '}';
        }
    }
} 