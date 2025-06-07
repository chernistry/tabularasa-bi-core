package com.tabularasa.bi.q1_realtime_stream_processing.controller;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import com.tabularasa.bi.q1_realtime_stream_processing.service.CampaignStatsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.http.MediaType;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(CampaignStatsController.class)
class CampaignStatsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CampaignStatsService campaignStatsService;

    @Test
    void getCampaignStats_shouldReturnStats_whenDataExists() throws Exception {
        String campaignId = "testCampaign";
        LocalDateTime startTime = LocalDateTime.of(2024, 7, 25, 10, 0, 0);
        LocalDateTime endTime = LocalDateTime.of(2024, 7, 25, 11, 0, 0);

        CampaignStatsDto mockStat = new CampaignStatsDto(startTime, campaignId, "click", 100L, 50.0);
        List<CampaignStatsDto> expectedStats = Collections.singletonList(mockStat);

        when(campaignStatsService.getCampaignStats(anyString(), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(expectedStats);

        mockMvc.perform(get("/api/v1/campaign-stats/{campaignId}", campaignId)
                        .param("startTime", startTime.toString())
                        .param("endTime", endTime.toString())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].campaignId").value(campaignId))
                .andExpect(jsonPath("$[0].eventCount").value(100L));
    }

    @Test
    void getCampaignStats_shouldReturnNotFound_whenNoDataExists() throws Exception {
        String campaignId = "nonExistentCampaign";
        LocalDateTime startTime = LocalDateTime.of(2024, 7, 25, 10, 0, 0);
        LocalDateTime endTime = LocalDateTime.of(2024, 7, 25, 11, 0, 0);

        when(campaignStatsService.getCampaignStats(anyString(), any(LocalDateTime.class), any(LocalDateTime.class)))
                .thenReturn(Collections.emptyList());

        mockMvc.perform(get("/api/v1/campaign-stats/{campaignId}", campaignId)
                        .param("startTime", startTime.toString())
                        .param("endTime", endTime.toString())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());
    }

    @Test
    void getCampaignStats_shouldReturnBadRequest_whenStartTimeIsAfterEndTime() throws Exception {
        String campaignId = "testCampaign";
        LocalDateTime startTime = LocalDateTime.of(2024, 7, 25, 11, 0, 0);
        LocalDateTime endTime = LocalDateTime.of(2024, 7, 25, 10, 0, 0); // endTime before startTime

        mockMvc.perform(get("/api/v1/campaign-stats/{campaignId}", campaignId)
                        .param("startTime", startTime.toString())
                        .param("endTime", endTime.toString())
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }
} 