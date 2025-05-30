package com.tabularasa.bi.q1_realtime_stream_processing.dto;

import java.time.LocalDateTime;

public class CampaignStatsDto {
    private LocalDateTime windowStartTime;
    private String campaignId;
    private String eventType;
    private long eventCount;
    private double totalBidAmount;

    // Constructors
    public CampaignStatsDto() {
    }

    public CampaignStatsDto(LocalDateTime windowStartTime, String campaignId, String eventType, long eventCount, double totalBidAmount) {
        this.windowStartTime = windowStartTime;
        this.campaignId = campaignId;
        this.eventType = eventType;
        this.eventCount = eventCount;
        this.totalBidAmount = totalBidAmount;
    }

    // Getters and Setters
    public LocalDateTime getWindowStartTime() {
        return windowStartTime;
    }

    public void setWindowStartTime(LocalDateTime windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(String campaignId) {
        this.campaignId = campaignId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getEventCount() {
        return eventCount;
    }

    public void setEventCount(long eventCount) {
        this.eventCount = eventCount;
    }

    public double getTotalBidAmount() {
        return totalBidAmount;
    }

    public void setTotalBidAmount(double totalBidAmount) {
        this.totalBidAmount = totalBidAmount;
    }

    // toString, equals, hashCode (optional, but good practice)
    @Override
    public String toString() {
        return "CampaignStatsDto{" +
                "windowStartTime=" + windowStartTime +
                ", campaignId='" + campaignId + "'" +
                ", eventType='" + eventType + "'" +
                ", eventCount=" + eventCount +
                ", totalBidAmount=" + totalBidAmount +
                '}';
    }
} 