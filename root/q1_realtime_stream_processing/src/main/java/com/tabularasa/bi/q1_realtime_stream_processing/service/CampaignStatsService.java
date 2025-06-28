package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Service for retrieving aggregated campaign statistics from the database.
 * Contains business logic for campaign statistics operations.
 */
@Service
public class CampaignStatsService {

    private static final Logger log = LoggerFactory.getLogger(CampaignStatsService.class);
    
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public CampaignStatsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Retrieves campaign statistics for a given campaign and time window.
     * Uses a read-only transaction for better performance.
     *
     * @param campaignId Campaign identifier
     * @param startTime Start of the time window
     * @param endTime End of the time window
     * @return List of campaign stats
     */
    @Transactional(readOnly = true)
    public List<CampaignStatsDto> getCampaignStats(String campaignId, LocalDateTime startTime, LocalDateTime endTime) {
        log.debug("Retrieving stats for campaign {} between {} and {}", campaignId, startTime, endTime);
        
        // Optimized SQL query with predicate pushdown
        String sql = "SELECT window_start_time, campaign_id, event_type, event_count, total_bid_amount " +
                     "FROM aggregated_campaign_stats " +
                     "WHERE campaign_id = ? " +
                     "AND window_start_time >= ? " +
                     "AND window_start_time < ? " +
                     "ORDER BY window_start_time ASC";

        Timestamp sqlStartTime = Timestamp.valueOf(startTime);
        Timestamp sqlEndTime = Timestamp.valueOf(endTime);
        
        log.trace("Executing SQL query: {}", sql);
        
        List<CampaignStatsDto> results = jdbcTemplate.query(sql,
            (rs, rowNum) -> new CampaignStatsDto(
                rs.getTimestamp("window_start_time").toLocalDateTime(),
                rs.getString("campaign_id"),
                rs.getString("event_type"),
                rs.getLong("event_count"),
                rs.getDouble("total_bid_amount")
            ),
            campaignId, sqlStartTime, sqlEndTime
        );
        
        log.debug("Found {} campaign stat records for campaign {}", results.size(), campaignId);
        return results;
    }
    
    /**
     * Retrieves campaign statistics with additional metrics for a given campaign and time window.
     * Example of using INNER JOIN for more complex queries.
     *
     * @param campaignId Campaign identifier
     * @param startTime Start of the time window
     * @param endTime End of the time window
     * @return List of campaign stats with additional metrics
     */
    @Transactional(readOnly = true)
    public List<CampaignStatsDto> getCampaignStatsWithMetrics(String campaignId, LocalDateTime startTime, LocalDateTime endTime) {
        log.debug("Retrieving stats with metrics for campaign {} between {} and {}", campaignId, startTime, endTime);
        
        // Example of optimized query with INNER JOIN
        String sql = "SELECT s.window_start_time, s.campaign_id, s.event_type, s.event_count, s.total_bid_amount " +
                     "FROM aggregated_campaign_stats s " +
                     "INNER JOIN campaign_metadata m ON s.campaign_id = m.campaign_id " +
                     "WHERE s.campaign_id = ? " +
                     "AND s.window_start_time >= ? " +
                     "AND s.window_start_time < ? " +
                     "AND m.is_active = true " +
                     "ORDER BY s.window_start_time ASC";

        // TODO: Implement campaign_metadata table and update this query when available
        
        log.info("Campaign metrics functionality not yet implemented, returning standard stats");
        return getCampaignStats(campaignId, startTime, endTime);
    }
}