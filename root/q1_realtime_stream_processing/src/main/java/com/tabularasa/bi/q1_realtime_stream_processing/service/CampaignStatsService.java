package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
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
     * Retrieves campaign statistics with additional metadata for active campaigns.
     * Uses INNER JOIN to combine stats with campaign metadata for enhanced analytics.
     *
     * @param campaignId Campaign identifier
     * @param startTime Start of the time window
     * @param endTime End of the time window
     * @return List of campaign stats with metadata validation
     */
    @Transactional(readOnly = true)
    public List<CampaignStatsDto> getCampaignStatsWithMetadata(String campaignId, LocalDateTime startTime, LocalDateTime endTime) {
        log.debug("Retrieving stats with metadata for campaign {} between {} and {}", campaignId, startTime, endTime);
        
        // Enhanced query with campaign metadata validation
        String sql = "SELECT s.window_start_time, s.campaign_id, s.event_type, s.event_count, s.total_bid_amount " +
                     "FROM aggregated_campaign_stats s " +
                     "INNER JOIN campaign_metadata m ON s.campaign_id = m.campaign_id " +
                     "WHERE s.campaign_id = ? " +
                     "AND s.window_start_time >= ? " +
                     "AND s.window_start_time < ? " +
                     "AND m.is_active = true " +
                     "AND (m.end_date IS NULL OR m.end_date >= CURRENT_DATE) " +
                     "ORDER BY s.window_start_time ASC";

        Timestamp sqlStartTime = Timestamp.valueOf(startTime);
        Timestamp sqlEndTime = Timestamp.valueOf(endTime);
        
        log.trace("Executing enhanced SQL query with metadata: {}", sql);
        
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
        
        log.debug("Found {} validated campaign stat records for campaign {}", results.size(), campaignId);
        return results;
    }
    
    /**
     * Retrieves all active campaigns for a given advertiser.
     * Useful for dashboard aggregations and reporting.
     *
     * @param advertiserId Advertiser identifier
     * @return List of active campaign IDs
     */
    @Transactional(readOnly = true)
    public List<String> getActiveCampaignsByAdvertiser(String advertiserId) {
        log.debug("Retrieving active campaigns for advertiser {}", advertiserId);
        
        String sql = "SELECT campaign_id FROM campaign_metadata " +
                     "WHERE advertiser_id = ? " +
                     "AND is_active = true " +
                     "AND (end_date IS NULL OR end_date >= CURRENT_DATE) " +
                     "ORDER BY priority ASC, campaign_name ASC";
        
        List<String> results = jdbcTemplate.queryForList(sql, String.class, advertiserId);
        
        log.debug("Found {} active campaigns for advertiser {}", results.size(), advertiserId);
        return results;
    }
    
    /**
     * Retrieves comprehensive campaign performance summary including metadata.
     * Provides aggregated metrics across all event types for reporting.
     *
     * @param campaignId Campaign identifier
     * @param startTime Start of the time window
     * @param endTime End of the time window
     * @return Campaign performance summary with metadata
     */
    @Transactional(readOnly = true)
    public CampaignPerformanceSummary getCampaignPerformanceSummary(String campaignId, LocalDateTime startTime, LocalDateTime endTime) {
        log.debug("Retrieving performance summary for campaign {} between {} and {}", campaignId, startTime, endTime);
        
        String sql = "SELECT " +
                     "    m.campaign_name, m.advertiser_name, m.industry_category, " +
                     "    m.budget_usd, m.target_ctr, m.target_cpa, " +
                     "    SUM(CASE WHEN s.event_type = 'impression' THEN s.event_count ELSE 0 END) as total_impressions, " +
                     "    SUM(CASE WHEN s.event_type = 'click' THEN s.event_count ELSE 0 END) as total_clicks, " +
                     "    SUM(CASE WHEN s.event_type = 'conversion' THEN s.event_count ELSE 0 END) as total_conversions, " +
                     "    SUM(s.total_bid_amount) as total_spend " +
                     "FROM campaign_metadata m " +
                     "LEFT JOIN aggregated_campaign_stats s ON m.campaign_id = s.campaign_id " +
                     "    AND s.window_start_time >= ? AND s.window_start_time < ? " +
                     "WHERE m.campaign_id = ? " +
                     "GROUP BY m.campaign_id, m.campaign_name, m.advertiser_name, m.industry_category, " +
                     "         m.budget_usd, m.target_ctr, m.target_cpa";
        
        Timestamp sqlStartTime = Timestamp.valueOf(startTime);
        Timestamp sqlEndTime = Timestamp.valueOf(endTime);
        
        return jdbcTemplate.queryForObject(sql, 
            (rs, rowNum) -> new CampaignPerformanceSummary(
                campaignId,
                rs.getString("campaign_name"),
                rs.getString("advertiser_name"),
                rs.getString("industry_category"),
                rs.getBigDecimal("budget_usd"),
                rs.getDouble("target_ctr"),
                rs.getBigDecimal("target_cpa"),
                rs.getLong("total_impressions"),
                rs.getLong("total_clicks"),
                rs.getLong("total_conversions"),
                rs.getDouble("total_spend")
            ),
            sqlStartTime, sqlEndTime, campaignId
        );
    }
    
    /**
     * Data class for campaign performance summary with metadata.
     */
    public static class CampaignPerformanceSummary {
        private final String campaignId;
        private final String campaignName;
        private final String advertiserName;
        private final String industryCategory;
        private final java.math.BigDecimal budgetUsd;
        private final Double targetCtr;
        private final java.math.BigDecimal targetCpa;
        private final Long totalImpressions;
        private final Long totalClicks;
        private final Long totalConversions;
        private final Double totalSpend;
        
        public CampaignPerformanceSummary(String campaignId, String campaignName, String advertiserName, 
                String industryCategory, java.math.BigDecimal budgetUsd, Double targetCtr, 
                java.math.BigDecimal targetCpa, Long totalImpressions, Long totalClicks, 
                Long totalConversions, Double totalSpend) {
            this.campaignId = campaignId;
            this.campaignName = campaignName;
            this.advertiserName = advertiserName;
            this.industryCategory = industryCategory;
            this.budgetUsd = budgetUsd;
            this.targetCtr = targetCtr;
            this.targetCpa = targetCpa;
            this.totalImpressions = totalImpressions;
            this.totalClicks = totalClicks;
            this.totalConversions = totalConversions;
            this.totalSpend = totalSpend;
        }
        
        // Getters
        public String getCampaignId() { return campaignId; }
        public String getCampaignName() { return campaignName; }
        public String getAdvertiserName() { return advertiserName; }
        public String getIndustryCategory() { return industryCategory; }
        public java.math.BigDecimal getBudgetUsd() { return budgetUsd; }
        public Double getTargetCtr() { return targetCtr; }
        public java.math.BigDecimal getTargetCpa() { return targetCpa; }
        public Long getTotalImpressions() { return totalImpressions; }
        public Long getTotalClicks() { return totalClicks; }
        public Long getTotalConversions() { return totalConversions; }
        public Double getTotalSpend() { return totalSpend; }
        
        // Calculated metrics
        public Double getActualCtr() {
            return totalImpressions > 0 ? (double) totalClicks / totalImpressions : 0.0;
        }
        
        public Double getActualCpa() {
            return totalConversions > 0 ? totalSpend / totalConversions : 0.0;
        }
        
        public Double getConversionRate() {
            return totalClicks > 0 ? (double) totalConversions / totalClicks : 0.0;
        }
    }
}