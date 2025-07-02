package com.tabularasa.bi.q1_realtime_stream_processing.dto;

import java.time.LocalDateTime;

/**
 * Data Transfer Object for aggregated campaign statistics.
 *
 * <p>Represents a single aggregation window for a campaign and event type.
 */
public class CampaignStatsDto {
    /**
     * The start time of the aggregation window.
     */
    private LocalDateTime windowStartTime;
    /**
     * The campaign identifier.
     */
    private String campaignId;
    /**
     * The type of event (e.g., click, conversion).
     */
    private String eventType;
    /**
     * The number of events in this window.
     */
    private long eventCount;
    /**
     * The total bid amount for events in this window.
     */
    private double totalBidAmount;

    /**
     * Default constructor.
     */
    public CampaignStatsDto() {
    }

    /**
     * Full constructor.
     *
     * @param windowStartTime Start time of the aggregation window.
     * @param campaignId      Campaign identifier.
     * @param eventType       Event type (e.g., click, conversion).
     * @param eventCount      Number of events in the window.
     * @param totalBidAmount  Total bid amount in the window.
     */
    public CampaignStatsDto(
            final LocalDateTime windowStartTime,
            final String campaignId,
            final String eventType,
            final long eventCount,
            final double totalBidAmount) {
        this.windowStartTime = windowStartTime;
        this.campaignId = campaignId;
        this.eventType = eventType;
        this.eventCount = eventCount;
        this.totalBidAmount = totalBidAmount;
    }

    /**
     * Gets the window start time.
     * @return The window start time.
     */
    public LocalDateTime getWindowStartTime() {
        return windowStartTime;
    }

    /**
     * Sets the window start time.
     * @param windowStartTime The window start time.
     */
    public void setWindowStartTime(final LocalDateTime windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    /**
     * Gets the campaign ID.
     * @return The campaign ID.
     */
    public String getCampaignId() {
        return campaignId;
    }

    /**
     * Sets the campaign ID.
     * @param campaignId The campaign ID.
     */
    public void setCampaignId(final String campaignId) {
        this.campaignId = campaignId;
    }

    /**
     * Gets the event type.
     * @return The event type.
     */
    public String getEventType() {
        return eventType;
    }

    /**
     * Sets the event type.
     * @param eventType The event type.
     */
    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    /**
     * Gets the event count.
     * @return The event count.
     */
    public long getEventCount() {
        return eventCount;
    }

    /**
     * Sets the event count.
     * @param eventCount The event count.
     */
    public void setEventCount(final long eventCount) {
        this.eventCount = eventCount;
    }

    /**
     * Gets the total bid amount.
     * @return The total bid amount.
     */
    public double getTotalBidAmount() {
        return totalBidAmount;
    }

    /**
     * Sets the total bid amount.
     * @param totalBidAmount The total bid amount.
     */
    public void setTotalBidAmount(final double totalBidAmount) {
        this.totalBidAmount = totalBidAmount;
    }

    @Override
    public String toString() {
        return "CampaignStatsDto{"
                + "windowStartTime=" + windowStartTime
                + ", campaignId='" + campaignId + '\''
                + ", eventType='" + eventType + '\''
                + ", eventCount=" + eventCount
                + ", totalBidAmount=" + totalBidAmount
                + '}';
    }
}