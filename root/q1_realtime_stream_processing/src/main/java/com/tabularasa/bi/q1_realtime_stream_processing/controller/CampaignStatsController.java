package com.tabularasa.bi.q1_realtime_stream_processing.controller;

import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import com.tabularasa.bi.q1_realtime_stream_processing.service.CampaignStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/v1/campaign-stats")
public class CampaignStatsController {

    private final CampaignStatsService campaignStatsService;

    @Autowired
    public CampaignStatsController(CampaignStatsService campaignStatsService) {
        this.campaignStatsService = campaignStatsService;
    }

    @GetMapping("/{campaignId}")
    public ResponseEntity<List<CampaignStatsDto>> getCampaignStats(
            @PathVariable String campaignId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endTime) {
        
        if (startTime.isAfter(endTime)) {
            return ResponseEntity.badRequest().build(); 
        }

        try {
            List<CampaignStatsDto> stats = campaignStatsService.getCampaignStats(campaignId, startTime, endTime);
            if (stats.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
} 