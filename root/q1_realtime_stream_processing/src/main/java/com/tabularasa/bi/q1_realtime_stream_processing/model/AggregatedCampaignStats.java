package com.tabularasa.bi.q1_realtime_stream_processing.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Сущность для хранения агрегированных статистических данных по рекламным кампаниям.
 * Содержит данные о показах, кликах и других событиях для каждой кампании за определенный временной промежуток.
 */
@Entity
@Table(name = "aggregated_campaign_stats")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AggregatedCampaignStats {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "campaign_id", nullable = false)
    private String campaignId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "event_count", nullable = false)
    private Long eventCount;

    @Column(name = "total_bid_amount", precision = 19, scale = 4)
    private BigDecimal totalBidAmount;

    @Column(name = "window_start_time", nullable = false)
    private LocalDateTime windowStartTime;

    @Column(name = "window_end_time", nullable = false)
    private LocalDateTime windowEndTime;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
} 