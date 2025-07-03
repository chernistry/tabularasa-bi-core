package com.tabularasa.bi.q1_realtime_stream_processing.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Entity representing aggregated campaign statistics.
 * Maps to the aggregated_campaign_stats table in the database.
 */
@Entity
@Table(name = "aggregated_campaign_stats")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AggregatedCampaignStats implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "campaign_id", nullable = false)
    private String campaignId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "window_start_time", nullable = false)
    private LocalDateTime windowStartTime;

    @Column(name = "window_end_time", nullable = false)
    private LocalDateTime windowEndTime;

    @Column(name = "event_count", nullable = false)
    private Long eventCount;

    @Column(name = "total_bid_amount", nullable = false, precision = 18, scale = 6)
    private BigDecimal totalBidAmount;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
} 