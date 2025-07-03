package com.tabularasa.bi.q1_realtime_stream_processing.controller;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import com.tabularasa.bi.q1_realtime_stream_processing.service.CampaignStatsService;
import java.time.LocalDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for retrieving campaign statistics.
 * This controller only handles HTTP requests and delegates business logic to the service layer.
 */
@RestController
@RequestMapping("/api/v1/campaign-stats")
public final class CampaignStatsController {

    private static final Logger log = LoggerFactory.getLogger(CampaignStatsController.class);
    
    private final CampaignStatsService campaignStatsService;

    /**
     * Constructs a new CampaignStatsController.
     *
     * @param campaignStatsService The campaign statistics service.
     */
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

        log.debug("Fetching stats for campaign {} from {} to {}", campaignId, startTime, endTime);
        
        if (startTime.isAfter(endTime)) {
            log.warn("Invalid time range request: start time {} is after end time {}", startTime, endTime);
            return ResponseEntity.badRequest().build();
        }

        try {
            final List<CampaignStatsDto> stats = campaignStatsService.getCampaignStats(campaignId, startTime, endTime);
            
            if (stats.isEmpty()) {
                log.info("No stats found for campaign {} in time range {} to {}", campaignId, startTime, endTime);
                return ResponseEntity.notFound().build();
            }
            
            log.debug("Retrieved {} stat entries for campaign {}", stats.size(), campaignId);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error retrieving campaign stats for campaign {}: {}", campaignId, e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
