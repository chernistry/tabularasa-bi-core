package com.tabularasa.bi.q1_realtime_stream_processing.model;

import lombok.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Entity class for storing aggregated campaign statistics.
 */
@Entity
@Table(name = "aggregated_campaign_stats")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class AggregatedCampaignStats {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

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

    @Column(name = "total_bid_amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal totalBidAmount;

    @Column(name = "last_updated", nullable = false)
    private LocalDateTime lastUpdated;
} 