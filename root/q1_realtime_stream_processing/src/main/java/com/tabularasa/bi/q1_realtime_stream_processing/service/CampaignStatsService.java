package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Service for retrieving aggregated campaign statistics from the database.
 */
@Service
public class CampaignStatsService {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public CampaignStatsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Retrieves campaign statistics for a given campaign and time window.
     *
     * @param campaignId Campaign identifier
     * @param startTime Start of the time window
     * @param endTime End of the time window
     * @return List of campaign stats
     */
    public List<CampaignStatsDto> getCampaignStats(String campaignId, LocalDateTime startTime, LocalDateTime endTime) {
        String sql = "SELECT window_start_time, campaign_id, event_type, event_count, total_bid_amount " +
                     "FROM aggregated_campaign_stats " +
                     "WHERE campaign_id = ? AND window_start_time >= ? AND window_start_time < ?";

        return jdbcTemplate.query(sql,
            (rs, rowNum) -> new CampaignStatsDto(
                rs.getTimestamp("window_start_time").toLocalDateTime(),
                rs.getString("campaign_id"),
                rs.getString("event_type"),
                rs.getLong("event_count"),
                rs.getDouble("total_bid_amount")
            ),
            campaignId, Timestamp.valueOf(startTime), Timestamp.valueOf(endTime)
        );
    }
} 