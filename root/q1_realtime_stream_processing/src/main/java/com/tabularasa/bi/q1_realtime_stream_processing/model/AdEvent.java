package com.tabularasa.bi.q1_realtime_stream_processing.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Model class representing an ad event from the Kafka stream.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdEvent {

    /**
     * The timestamp when the event occurred.
     */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date timestamp;

    /**
     * The unique identifier of the advertising campaign.
     */
    @JsonProperty("campaign_id")
    private String campaignId;

    /**
     * A unique identifier for the event itself.
     */
    @JsonProperty("event_id")
    private String eventId;

    /**
     * Identifier for the ad creative shown.
     */
    @JsonProperty("ad_creative_id")
    private String adCreativeId;

    /**
     * The type of event (e.g., impression, click, conversion).
     */
    @JsonProperty("event_type")
    private String eventType;

    /**
     * The unique identifier of the user who triggered the event.
     */
    @JsonProperty("user_id")
    private String userId;

    /**
     * The bid amount in USD associated with this event.
     */
    @JsonProperty("bid_amount_usd")
    private BigDecimal bidAmountUsd;
}