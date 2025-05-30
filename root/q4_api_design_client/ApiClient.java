package com.tabularasa.bi.q4_api_design_client;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Client for the Taboola BI Events API.
 * This client allows sending batches of events to the BI platform.
 */
public class ApiClient {
    private static final String DEFAULT_API_BASE_URL = "http://localhost:12345";
    private static final String BI_EVENTS_ENDPOINT = "/v1/bi-events";
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String apiBaseUrl;
    private final String authToken;

    /**
     * Creates a new ApiClient with custom settings.
     * 
     * @param apiBaseUrl Base URL for the API (e.g., "https://api.taboola.com")
     * @param authToken JWT authentication token
     */
    public ApiClient(String apiBaseUrl, String authToken) {
        this.apiBaseUrl = apiBaseUrl;
        this.authToken = authToken;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(DEFAULT_TIMEOUT)
                .build();
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Creates a new ApiClient with default mock settings for demonstration.
     * In a real scenario, valid URL and token would be required.
     */
    public ApiClient() {
        this(DEFAULT_API_BASE_URL, "mock-auth-token");
    }

    /**
     * Sends a batch of events to the BI platform.
     * 
     * @param source Source system identifier
     * @param events List of events to send
     * @return Response details containing status and any error information
     * @throws IOException If there was an issue with the request
     * @throws InterruptedException If the operation was interrupted
     */
    public ApiResponse sendEvents(String source, List<Event> events) throws IOException, InterruptedException {
        String batchId = UUID.randomUUID().toString();
        return sendEvents(batchId, source, events);
    }

    /**
     * Sends a batch of events to the BI platform with a specific batch ID.
     * 
     * @param batchId Unique identifier for this batch
     * @param source Source system identifier
     * @param events List of events to send
     * @return Response details containing status and any error information
     * @throws IOException If there was an issue with the request
     * @throws InterruptedException If the operation was interrupted
     */
    public ApiResponse sendEvents(String batchId, String source, List<Event> events) 
            throws IOException, InterruptedException {
        // Prepare the request payload
        ObjectNode rootNode = objectMapper.createObjectNode();
        rootNode.put("batch_id", batchId);
        rootNode.put("source", source);
        
        ArrayNode eventsNode = rootNode.putArray("events");
        for (Event event : events) {
            ObjectNode eventNode = eventsNode.addObject();
            eventNode.put("event_id", event.getEventId());
            eventNode.put("timestamp", event.getTimestamp().toString());
            eventNode.put("event_name", event.getEventName());
            eventNode.set("attributes", objectMapper.valueToTree(event.getAttributes()));
        }
        
        String jsonPayload = objectMapper.writeValueAsString(rootNode);
        
        // Build and send the HTTP request
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiBaseUrl + BI_EVENTS_ENDPOINT))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + authToken)
                .header("X-Request-ID", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();
        
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            return new ApiResponse(false, 0, "Failed to connect to API: " + e.getMessage(), null);
        }
        
        // Process the response
        int statusCode = response.statusCode();
        Map<String, Object> responseData = null;
        if (!response.body().isEmpty()) {
            try {
                responseData = objectMapper.readValue(response.body(), 
                    objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class));
            } catch (Exception e) {
                responseData = Map.of("raw_response", response.body());
            }
        }
        
        boolean success = statusCode >= 200 && statusCode < 300;
        String message = success ? "Events sent successfully" : 
                            "API returned error status " + statusCode;
        
        return new ApiResponse(success, statusCode, message, responseData);
    }

    /**
     * For demonstration purposes, this method mocks sending events without actually
     * making an HTTP request. It simulates both success and error scenarios.
     * 
     * @param source Source system identifier
     * @param events List of events to send
     * @param simulateError If true, simulate an error response
     * @return Mocked API response
     */
    public ApiResponse mockSendEvents(String source, List<Event> events, boolean simulateError) {
        if (simulateError) {
            Map<String, Object> errorDetails = new HashMap<>();
            errorDetails.put("status", "error");
            errorDetails.put("code", "validation_error");
            errorDetails.put("message", "Simulated validation error");
            List<Map<String, String>> details = new ArrayList<>();
            details.add(Map.of("field", "events[0].timestamp", "error", "Invalid timestamp format"));
            errorDetails.put("details", details);
            errorDetails.put("request_id", UUID.randomUUID().toString());

            return new ApiResponse(false, 400, "API returned validation error", errorDetails);
        } else {
            String batchId = UUID.randomUUID().toString();
            Map<String, Object> successDetails = new HashMap<>();
            successDetails.put("status", "accepted");
            successDetails.put("batch_id", batchId);
            successDetails.put("message", "Batch accepted for processing");
            successDetails.put("request_id", UUID.randomUUID().toString());

            return new ApiResponse(true, 202, "Events sent successfully", successDetails);
        }
    }

    /**
     * Represents an event to be sent to the BI platform.
     */
    public static class Event {
        private final String eventId;
        private final Instant timestamp;
        private final String eventName;
        private final Map<String, Object> attributes;

        public Event(String eventId, Instant timestamp, String eventName, Map<String, Object> attributes) {
            this.eventId = eventId;
            this.timestamp = timestamp;
            this.eventName = eventName;
            this.attributes = attributes;
        }

        public String getEventId() { return eventId; }
        public Instant getTimestamp() { return timestamp; }
        public String getEventName() { return eventName; }
        public Map<String, Object> getAttributes() { return attributes; }
    }

    /**
     * Represents a response from the BI Events API.
     */
    public static class ApiResponse {
        private final boolean success;
        private final int statusCode;
        private final String message;
        private final Map<String, Object> data;

        public ApiResponse(boolean success, int statusCode, String message, Map<String, Object> data) {
            this.success = success;
            this.statusCode = statusCode;
            this.message = message;
            this.data = data;
        }

        public boolean isSuccess() { return success; }
        public int getStatusCode() { return statusCode; }
        public String getMessage() { return message; }
        public Map<String, Object> getData() { return data; }

        @Override
        public String toString() {
            return String.format("ApiResponse{success=%s, statusCode=%d, message='%s', data=%s}",
                    success, statusCode, message, data);
        }
    }

    /**
     * Main method demonstrating client usage.
     */
    public static void main(String[] args) {
        // Create a client
        ApiClient client = new ApiClient();
        
        // Prepare example event data
        List<Event> events = new ArrayList<>();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("campaign_id", "camp123");
        attributes.put("impression_id", "imp456");
        attributes.put("publisher_id", "pub789");
        attributes.put("device_type", "mobile");
        attributes.put("country", "US");
        attributes.put("bid_amount_usd", 0.025);
        
        events.add(new Event(
            UUID.randomUUID().toString(),
            Instant.now(),
            "impression",
            attributes
        ));
        
        // Try sending with success scenario
        System.out.println("Sending events (success scenario)...");
        ApiResponse successResponse = client.mockSendEvents("campaign_tracker", events, false);
        System.out.println(successResponse);
        
        // Try sending with error scenario
        System.out.println("\nSending events (error scenario)...");
        ApiResponse errorResponse = client.mockSendEvents("campaign_tracker", events, true);
        System.out.println(errorResponse);
        
        // Note: Actual HTTP call is not made here due to mock server URL
        // The code below would be used with a real server:
        /*
        try {
            ApiResponse response = client.sendEvents("campaign_tracker", events);
            System.out.println(response);
        } catch (Exception e) {
            System.err.println("Error sending events: " + e.getMessage());
            e.printStackTrace();
        }
        */
    }
} 