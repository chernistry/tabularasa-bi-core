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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.lenient;
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

    @BeforeEach
    void setUp() {
        campaignId = "testCampaign";
        startTime = LocalDateTime.of(2024, 7, 25, 10, 0, 0);
        endTime = LocalDateTime.of(2024, 7, 25, 11, 0, 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCampaignStats_shouldReturnListOfStats_whenDataExists() {
        // Given
        CampaignStatsDto mockStat = new CampaignStatsDto(startTime, campaignId, "click", 100L, 50.0);
        List<CampaignStatsDto> expectedStats = Collections.singletonList(mockStat);

        // Use lenient stubbing to avoid strict stubbing issues
        lenient().when(jdbcTemplate.query(
                anyString(),
                any(RowMapper.class),
                eq(campaignId),
                any(Timestamp.class),
                any(Timestamp.class)
        )).thenReturn(expectedStats);

        // When
        List<CampaignStatsDto> actualStats = campaignStatsService.getCampaignStats(campaignId, startTime, endTime);

        // Then
        assertNotNull(actualStats);
        assertEquals(1, actualStats.size());
        assertEquals(expectedStats.get(0), actualStats.get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCampaignStats_shouldReturnEmptyList_whenNoDataExists() {
        // Given
        List<CampaignStatsDto> emptyList = Collections.emptyList();
        
        // Use lenient stubbing to avoid strict stubbing issues
        lenient().when(jdbcTemplate.query(
                anyString(),
                any(RowMapper.class),
                eq(campaignId),
                any(Timestamp.class),
                any(Timestamp.class)
        )).thenReturn(emptyList);

        // When
        List<CampaignStatsDto> actualStats = campaignStatsService.getCampaignStats(campaignId, startTime, endTime);

        // Then
        assertNotNull(actualStats);
        assertTrue(actualStats.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCampaignStats_shouldHandleNullCampaignId() {
        // Given
        List<CampaignStatsDto> emptyList = Collections.emptyList();
        lenient().when(jdbcTemplate.query(
                anyString(),
                any(RowMapper.class),
                any(),
                any(Timestamp.class),
                any(Timestamp.class)
        )).thenReturn(emptyList);

        // When
        List<CampaignStatsDto> actualStats = campaignStatsService.getCampaignStats(null, startTime, endTime);

        // Then
        assertNotNull(actualStats);
        assertTrue(actualStats.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCampaignStats_shouldHandleNullTimeParameters() {
        // Given
        List<CampaignStatsDto> emptyList = Collections.emptyList();
        lenient().when(jdbcTemplate.query(
                anyString(),
                any(RowMapper.class),
                eq(campaignId),
                any(),
                any()
        )).thenReturn(emptyList);

        // When/Then - Service handles null gracefully by returning empty results
        List<CampaignStatsDto> statsWithNullStart = campaignStatsService.getCampaignStats(campaignId, null, endTime);
        assertNotNull(statsWithNullStart);
        assertTrue(statsWithNullStart.isEmpty());
        
        List<CampaignStatsDto> statsWithNullEnd = campaignStatsService.getCampaignStats(campaignId, startTime, null);
        assertNotNull(statsWithNullEnd);
        assertTrue(statsWithNullEnd.isEmpty());
    }
}