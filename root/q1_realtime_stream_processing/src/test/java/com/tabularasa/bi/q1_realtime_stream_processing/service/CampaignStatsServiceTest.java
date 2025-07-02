package com.tabularasa.bi.q1_realtime_stream_processing.service;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CampaignStatsServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private CampaignStatsService campaignStatsService;

    private String campaignId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String sqlQuery;

    @BeforeEach
    void setUp() {
        campaignId = "testCampaign";
        startTime = LocalDateTime.of(2024, 7, 25, 10, 0, 0);
        endTime = LocalDateTime.of(2024, 7, 25, 11, 0, 0);

        sqlQuery = "SELECT window_start_time, campaign_id, event_type, event_count, total_bid_amount " +
                     "FROM aggregated_campaign_stats " +
                     "WHERE campaign_id = ? AND window_start_time >= ? AND window_start_time < ?";
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCampaignStats_shouldReturnListOfStats_whenDataExists() {
        CampaignStatsDto mockStat = new CampaignStatsDto(startTime, campaignId, "click", 100L, 50.0);
        List<CampaignStatsDto> expectedStats = Collections.singletonList(mockStat);

        when(jdbcTemplate.query(eq(sqlQuery), (RowMapper<CampaignStatsDto>) any(), eq(campaignId), eq(Timestamp.valueOf(startTime)), eq(Timestamp.valueOf(endTime))))
                .thenReturn(expectedStats);

        List<CampaignStatsDto> actualStats = campaignStatsService.getCampaignStats(campaignId, startTime, endTime);

        assertNotNull(actualStats);
        assertEquals(1, actualStats.size());
        assertEquals(expectedStats.get(0), actualStats.get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCampaignStats_shouldReturnEmptyList_whenNoDataExists() {
        List<CampaignStatsDto> emptyList = Collections.emptyList();
        when(jdbcTemplate.query(eq(sqlQuery), (RowMapper<CampaignStatsDto>) any(), eq(campaignId), eq(Timestamp.valueOf(startTime)), eq(Timestamp.valueOf(endTime))))
                .thenReturn(emptyList);

        List<CampaignStatsDto> actualStats = campaignStatsService.getCampaignStats(campaignId, startTime, endTime);

        assertNotNull(actualStats);
        assertTrue(actualStats.isEmpty());
    }
}