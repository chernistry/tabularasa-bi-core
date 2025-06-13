package com.tabularasa.bi.q1_realtime_stream_processing.controller;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import com.tabularasa.bi.q1_realtime_stream_processing.service.CampaignStatsService;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for retrieving campaign statistics.
 */
@RestController
@RequestMapping("/api/v1/campaign-stats")
public final class CampaignStatsController {

    /**
     * Service for campaign statistics.
     */
    private final CampaignStatsService campaignStatsService;

    /**
     * Constructs a new CampaignStatsController.
     *
     * @param campaignStatsService The campaign statistics service.
     */
    @Autowired
    public CampaignStatsController(final CampaignStatsService campaignStatsService) {
        this.campaignStatsService = campaignStatsService;
    }

    /**
     * Gets campaign statistics for a given campaign and time window.
     *
     * @param campaignId The campaign identifier.
     * @param startTime  The start of the time window.
     * @param endTime    The end of the time window.
     * @return A list of campaign statistics.
     */
    @GetMapping("/{campaignId}")
    public ResponseEntity<List<CampaignStatsDto>> getCampaignStats(
            @PathVariable final String campaignId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            final LocalDateTime startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            final LocalDateTime endTime) {

        if (startTime.isAfter(endTime)) {
            return ResponseEntity.badRequest().build();
        }

        try {
            final List<CampaignStatsDto> stats =
                    campaignStatsService.getCampaignStats(campaignId, startTime, endTime);
            if (stats.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            // In a real app, you'd want more specific exception handling and logging
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
