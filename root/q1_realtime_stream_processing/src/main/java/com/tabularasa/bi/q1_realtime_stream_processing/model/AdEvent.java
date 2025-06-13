package com.tabularasa.bi.q1_realtime_stream_processing.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents an advertisement event.
 * This class is a simple Plain Old Java Object (POJO) that is serializable.
 */
public class AdEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The unique identifier for the event.
     */
    private String eventId;

    /**
     * The identifier for the ad creative.
     */
    private String adCreativeId;

    /**
     * The identifier for the user.
     */
    private String userId;

    /**
     * The type of the event (e.g., "impression", "click").
     */
    private String eventType;

    /**
     * The timestamp of when the event occurred.
     */
    private LocalDateTime timestamp;

    /**
     * Gets the event ID.
     * @return The event ID.
     */
    public String getEventId() {
        return eventId;
    }

    /**
     * Sets the event ID.
     * @param eventId The event ID to set.
     */
    public void setEventId(final String eventId) {
        this.eventId = eventId;
    }

    /**
     * Gets the ad creative ID.
     * @return The ad creative ID.
     */
    public String getAdCreativeId() {
        return adCreativeId;
    }

    /**
     * Sets the ad creative ID.
     * @param adCreativeId The ad creative ID to set.
     */
    public void setAdCreativeId(final String adCreativeId) {
        this.adCreativeId = adCreativeId;
    }

    /**
     * Gets the user ID.
     * @return The user ID.
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Sets the user ID.
     * @param userId The user ID to set.
     */
    public void setUserId(final String userId) {
        this.userId = userId;
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
     * @param eventType The event type to set.
     */
    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    /**
     * Gets the event timestamp.
     * @return The event timestamp.
     */
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the event timestamp.
     * @param timestamp The event timestamp to set.
     */
    public void setTimestamp(final LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AdEvent adEvent = (AdEvent) o;
        return Objects.equals(eventId, adEvent.eventId)
                && Objects.equals(adCreativeId, adEvent.adCreativeId)
                && Objects.equals(userId, adEvent.userId)
                && Objects.equals(eventType, adEvent.eventType)
                && Objects.equals(timestamp, adEvent.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, adCreativeId, userId, eventType, timestamp);
    }

    @Override
    public String toString() {
        return "AdEvent{"
                + "eventId='" + eventId + '\''
                + ", adCreativeId='" + adCreativeId + '\''
                + ", userId='" + userId + '\''
                + ", eventType='" + eventType + '\''
                + ", timestamp=" + timestamp
                + '}';
    }
}