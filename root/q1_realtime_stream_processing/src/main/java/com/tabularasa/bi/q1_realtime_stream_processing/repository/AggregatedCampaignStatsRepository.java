package com.tabularasa.bi.q1_realtime_stream_processing.repository;

import com.tabularasa.bi.q1_realtime_stream_processing.model.AggregatedCampaignStats;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository interface for AggregatedCampaignStats entities.
 */
@Repository
public interface AggregatedCampaignStatsRepository extends JpaRepository<AggregatedCampaignStats, Integer> {

    /**
     * Find statistics for a specific campaign and event type within a time window.
     *
     * @param campaignId Campaign identifier
     * @param eventType Type of event (impression, click, etc)
     * @param startTime Start time of the window
     * @param endTime End time of the window
     * @return An optional containing the stats if found
     */
    Optional<AggregatedCampaignStats> findByCampaignIdAndEventTypeAndWindowStartTimeAndWindowEndTime(
            String campaignId, String eventType, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * Find all statistics for a specific campaign.
     *
     * @param campaignId Campaign identifier
     * @return List of aggregated statistics
     */
    List<AggregatedCampaignStats> findByCampaignId(String campaignId);

    /**
     * Find all statistics for a specific event type.
     *
     * @param eventType Type of event (impression, click, etc)
     * @return List of aggregated statistics
     */
    List<AggregatedCampaignStats> findByEventType(String eventType);
    
    /**
     * Find statistics within a specific time range.
     *
     * @param startTime Start of the time range
     * @param endTime End of the time range
     * @return List of aggregated statistics
     */
    @Query(value = "SELECT * FROM aggregated_campaign_stats WHERE window_start_time >= :startTime AND window_end_time <= :endTime", nativeQuery = true)
    List<AggregatedCampaignStats> findInTimeRange(@Param("startTime") LocalDateTime startTime, @Param("endTime") LocalDateTime endTime);
} 