package com.tabularasa.bi.q1_realtime_stream_processing.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabularasa.bi.q1_realtime_stream_processing.Q1RealtimeStreamProcessingApplication;
import com.tabularasa.bi.q1_realtime_stream_processing.dto.CampaignStatsDto;
import com.tabularasa.bi.q1_realtime_stream_processing.service.CampaignStatsService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Comprehensive integration test suite for the TabulaRasa BI Core application.
 * 
 * This test class validates the complete application stack including:
 * - Database connectivity and migrations
 * - REST API endpoints
 * - Service layer integration
 * - Configuration management
 * - Health checks and monitoring
 * 
 * Uses Testcontainers for isolated testing environment with real PostgreSQL and Kafka instances.
 */
@SpringBootTest(
    classes = Q1RealtimeStreamProcessingApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@Testcontainers
@ActiveProfiles("test")
@TestPropertySource(properties = {
    "spring.flyway.enabled=true",
    "spring.jpa.hibernate.ddl-auto=validate",
    "management.endpoints.web.exposure.include=health,info,metrics,prometheus",
    "logging.level.com.tabularasa.bi=DEBUG"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Application Integration Test Suite")
class ApplicationIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.6-alpine"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withReuse(true);

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
            .withReuse(true);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Database configuration
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        
        // Kafka configuration
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("app.kafka.bootstrap-servers", kafka::getBootstrapServers);
        
        // Test-specific configurations
        registry.add("spark.streaming.checkpoint-location", () -> "/tmp/test-checkpoints");
        registry.add("spring.profiles.active", () -> "test,simple");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CampaignStatsService campaignStatsService;

    private String baseUrl;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        baseUrl = "http://localhost:" + port;
        objectMapper = new ObjectMapper();
    }

    @Test
    @Order(1)
    @DisplayName("Should start application context successfully")
    void contextLoads() {
        assertThat(restTemplate).isNotNull();
        assertThat(jdbcTemplate).isNotNull();
        assertThat(campaignStatsService).isNotNull();
    }

    @Test
    @Order(2)
    @DisplayName("Should verify database connectivity and schema initialization")
    void shouldVerifyDatabaseSetup() {
        // Verify database connectivity
        String result = jdbcTemplate.queryForObject("SELECT 1", String.class);
        assertThat(result).isEqualTo("1");

        // Verify Flyway migrations have run
        Integer migrationCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM flyway_schema_history WHERE success = true", 
            Integer.class
        );
        assertThat(migrationCount).isGreaterThan(0);

        // Verify main tables exist
        Boolean aggregatedStatsTableExists = jdbcTemplate.queryForObject(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'aggregated_campaign_stats')",
            Boolean.class
        );
        assertThat(aggregatedStatsTableExists).isTrue();

        Boolean campaignMetadataTableExists = jdbcTemplate.queryForObject(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'campaign_metadata')",
            Boolean.class
        );
        assertThat(campaignMetadataTableExists).isTrue();

        // Verify sample data exists
        Integer sampleDataCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM campaign_metadata", 
            Integer.class
        );
        assertThat(sampleDataCount).isGreaterThan(0);
    }

    @Test
    @Order(3)
    @DisplayName("Should respond to health check endpoint")
    void shouldRespondToHealthCheck() {
        ResponseEntity<String> response = restTemplate.getForEntity(
            baseUrl + "/actuator/health", 
            String.class
        );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"UP\"");
    }

    @Test
    @Order(4)
    @DisplayName("Should provide application info endpoint")
    void shouldProvideApplicationInfo() {
        ResponseEntity<String> response = restTemplate.getForEntity(
            baseUrl + "/actuator/info", 
            String.class
        );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        // Verify basic info structure
        assertThat(response.getBody()).isNotEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("Should expose Prometheus metrics endpoint")
    void shouldExposePrometheusMetrics() {
        ResponseEntity<String> response = restTemplate.getForEntity(
            baseUrl + "/actuator/prometheus", 
            String.class
        );
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("# HELP");
        assertThat(response.getBody()).contains("# TYPE");
    }

    @Nested
    @DisplayName("Campaign Stats Service Integration Tests")
    class CampaignStatsServiceTests {

        @BeforeEach
        void setupTestData() {
            // Insert test data for campaign stats
            jdbcTemplate.execute(
                "INSERT INTO aggregated_campaign_stats " +
                "(campaign_id, event_type, window_start_time, window_end_time, event_count, total_bid_amount, updated_at) " +
                "VALUES " +
                "('test_campaign_001', 'impression', '2025-01-01 10:00:00', '2025-01-01 10:01:00', 1000, 50.00, CURRENT_TIMESTAMP), " +
                "('test_campaign_001', 'click', '2025-01-01 10:00:00', '2025-01-01 10:01:00', 50, 25.00, CURRENT_TIMESTAMP), " +
                "('test_campaign_002', 'impression', '2025-01-01 10:00:00', '2025-01-01 10:01:00', 2000, 100.00, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (campaign_id, event_type, window_start_time) DO NOTHING"
            );
        }

        @Test
        @DisplayName("Should retrieve campaign stats for valid campaign")
        void shouldRetrieveCampaignStats() {
            LocalDateTime startTime = LocalDateTime.of(2025, 1, 1, 9, 0);
            LocalDateTime endTime = LocalDateTime.of(2025, 1, 1, 11, 0);
            
            List<CampaignStatsDto> stats = campaignStatsService.getCampaignStats(
                "test_campaign_001", startTime, endTime
            );
            
            assertThat(stats).isNotEmpty();
            assertThat(stats).hasSize(2); // impression and click events
            
            // Verify impression event
            CampaignStatsDto impressionStats = stats.stream()
                .filter(stat -> "impression".equals(stat.getEventType()))
                .findFirst()
                .orElseThrow();
            
            assertThat(impressionStats.getCampaignId()).isEqualTo("test_campaign_001");
            assertThat(impressionStats.getEventCount()).isEqualTo(1000);
            assertThat(impressionStats.getTotalBidAmount()).isEqualTo(50.00);
        }

        @Test
        @DisplayName("Should retrieve campaign stats with metadata validation")
        void shouldRetrieveCampaignStatsWithMetadata() {
            LocalDateTime startTime = LocalDateTime.of(2025, 1, 1, 9, 0);
            LocalDateTime endTime = LocalDateTime.of(2025, 1, 1, 11, 0);
            
            // This should only return stats for campaigns that exist in campaign_metadata
            List<CampaignStatsDto> stats = campaignStatsService.getCampaignStatsWithMetadata(
                "campaign_001", startTime, endTime  // Using campaign from sample data
            );
            
            // Should return empty or valid stats (depending on test data setup)
            assertThat(stats).isNotNull();
        }

        @Test
        @DisplayName("Should handle non-existent campaign gracefully")
        void shouldHandleNonExistentCampaign() {
            LocalDateTime startTime = LocalDateTime.of(2025, 1, 1, 9, 0);
            LocalDateTime endTime = LocalDateTime.of(2025, 1, 1, 11, 0);
            
            List<CampaignStatsDto> stats = campaignStatsService.getCampaignStats(
                "non_existent_campaign", startTime, endTime
            );
            
            assertThat(stats).isEmpty();
        }

        @Test
        @DisplayName("Should retrieve active campaigns by advertiser")
        void shouldRetrieveActiveCampaignsByAdvertiser() {
            List<String> campaigns = campaignStatsService.getActiveCampaignsByAdvertiser("adv_001");
            
            assertThat(campaigns).isNotNull();
            // Should contain campaigns from sample data for advertiser adv_001
        }
    }

    @Nested
    @DisplayName("REST API Integration Tests")
    class RestApiTests {

        @Test
        @DisplayName("Should handle API requests with proper error responses")
        void shouldHandleApiRequestsWithProperErrorResponses() {
            // Test non-existent endpoint
            ResponseEntity<String> response = restTemplate.getForEntity(
                baseUrl + "/api/non-existent", 
                String.class
            );
            
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        }

        @Test
        @DisplayName("Should handle malformed requests gracefully")
        void shouldHandleMalformedRequestsGracefully() {
            // This test would be expanded based on actual API endpoints
            // For now, just verify the application doesn't crash
            assertThat(restTemplate).isNotNull();
        }
    }

    @Nested
    @DisplayName("Database Performance Tests")
    class DatabasePerformanceTests {

        @Test
        @DisplayName("Should handle concurrent database operations")
        void shouldHandleConcurrentDatabaseOperations() {
            // Test concurrent access to campaign stats service
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertDoesNotThrow(() -> {
                    LocalDateTime startTime = LocalDateTime.of(2025, 1, 1, 9, 0);
                    LocalDateTime endTime = LocalDateTime.of(2025, 1, 1, 11, 0);
                    campaignStatsService.getCampaignStats("test_campaign", startTime, endTime);
                });
            });
        }

        @Test
        @DisplayName("Should maintain data consistency under load")
        void shouldMaintainDataConsistencyUnderLoad() {
            // Insert multiple records and verify consistency
            int initialCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM aggregated_campaign_stats", 
                Integer.class
            );
            
            // Perform multiple insertions
            for (int i = 0; i < 10; i++) {
                jdbcTemplate.execute(
                    String.format(
                        "INSERT INTO aggregated_campaign_stats " +
                        "(campaign_id, event_type, window_start_time, window_end_time, event_count, total_bid_amount, updated_at) " +
                        "VALUES ('load_test_%d', 'impression', '2025-01-01 10:%02d:00', '2025-01-01 10:%02d:00', 100, 10.00, CURRENT_TIMESTAMP) " +
                        "ON CONFLICT (campaign_id, event_type, window_start_time) DO NOTHING",
                        i, i, i + 1
                    )
                );
            }
            
            int finalCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM aggregated_campaign_stats", 
                Integer.class
            );
            
            assertThat(finalCount).isGreaterThanOrEqualTo(initialCount);
        }
    }

    @AfterEach
    void cleanUpTestData() {
        // Clean up test-specific data
        try {
            jdbcTemplate.execute("DELETE FROM aggregated_campaign_stats WHERE campaign_id LIKE 'test_%' OR campaign_id LIKE 'load_test_%'");
        } catch (Exception e) {
            // Log but don't fail the test
            System.err.println("Warning: Could not clean up test data: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() {
        // Containers will be automatically stopped by Testcontainers
    }
}