package com.tabularasa.bi.q4_api_design_client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the BI Events API client.
 * These tests primarily focus on the mock functionality
 * since we can't actually connect to a server in this environment.
 */
class ApiClientTest {

    private ApiClient apiClient;
    private List<ApiClient.Event> testEvents;
    private String testSource;

    @BeforeEach
    void setUp() {
        // Initialize the API client
        apiClient = new ApiClient();
        
        // Create some test events
        testEvents = new ArrayList<>();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("campaign_id", "test123");
        attributes.put("impression_id", "imp456");
        attributes.put("bid_amount_usd", 0.05);
        
        testEvents.add(new ApiClient.Event(
            UUID.randomUUID().toString(),
            Instant.now(),
            "impression",
            attributes
        ));
        
        testSource = "test_source";
    }

    @Test
    void mockSendEvents_successScenario_shouldReturnSuccessResponse() {
        // Test the success scenario
        ApiClient.ApiResponse response = apiClient.mockSendEvents(testSource, testEvents, false);
        
        // Verify the response
        assertTrue(response.isSuccess());
        assertEquals(202, response.getStatusCode());
        assertNotNull(response.getData());
        assertEquals("accepted", response.getData().get("status"));
        assertNotNull(response.getData().get("batch_id"));
        assertNotNull(response.getData().get("request_id"));
    }

    @Test
    void mockSendEvents_errorScenario_shouldReturnErrorResponse() {
        // Test the error scenario
        ApiClient.ApiResponse response = apiClient.mockSendEvents(testSource, testEvents, true);
        
        // Verify the response
        assertFalse(response.isSuccess());
        assertEquals(400, response.getStatusCode());
        assertNotNull(response.getData());
        assertEquals("error", response.getData().get("status"));
        assertEquals("validation_error", response.getData().get("code"));
        assertNotNull(response.getData().get("details"));
    }

    @Test
    void event_constructor_shouldSetAllFields() {
        // Create test data
        String eventId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();
        String eventName = "test_event";
        Map<String, Object> attributes = Map.of("key1", "value1", "key2", 123);
        
        // Create the event
        ApiClient.Event event = new ApiClient.Event(eventId, timestamp, eventName, attributes);
        
        // Verify all fields are set correctly
        assertEquals(eventId, event.getEventId());
        assertEquals(timestamp, event.getTimestamp());
        assertEquals(eventName, event.getEventName());
        assertEquals(attributes, event.getAttributes());
    }

    @Test
    void apiResponse_constructor_shouldSetAllFields() {
        // Create test data
        boolean success = true;
        int statusCode = 202;
        String message = "Test message";
        Map<String, Object> data = Map.of("key1", "value1", "key2", 123);
        
        // Create the response
        ApiClient.ApiResponse response = new ApiClient.ApiResponse(success, statusCode, message, data);
        
        // Verify all fields are set correctly
        assertEquals(success, response.isSuccess());
        assertEquals(statusCode, response.getStatusCode());
        assertEquals(message, response.getMessage());
        assertEquals(data, response.getData());
    }

    @Test
    void apiResponse_toString_shouldIncludeAllFields() {
        // Create a response
        ApiClient.ApiResponse response = new ApiClient.ApiResponse(true, 202, "Test", Map.of("key", "value"));
        
        // Get the string representation
        String str = response.toString();
        
        // Verify it includes all fields
        assertTrue(str.contains("success=true"));
        assertTrue(str.contains("statusCode=202"));
        assertTrue(str.contains("message='Test'"));
        assertTrue(str.contains("data={key=value}"));
    }
} 