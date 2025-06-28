package com.tabularasa.bi.q1_realtime_stream_processing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * Represents an ad event from the Kafka stream.
 * This class is used for deserializing JSON data from Kafka messages.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdEvent implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Timestamp when the event occurred
     */
    private Instant timestamp;
    
    /**
     * Unique identifier for the campaign
     */
    private String campaignId;
    
    /**
     * Unique identifier for this specific event
     */
    private String eventId;
    
    /**
     * Identifier for the ad creative
     */
    private String adCreativeId;
    
    /**
     * Type of event (impression, click, etc.)
     */
    private String eventType;
    
    /**
     * Identifier for the user who triggered the event
     */
    private String userId;
    
    /**
     * The bid amount in USD for this event
     */
    private Double bidAmountUsd;
}