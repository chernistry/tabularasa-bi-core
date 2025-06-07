package com.tabularasa.bi.q1_realtime_stream_processing.kafka.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Application-level Kafka configuration properties.
 */
@Configuration
@Getter
public class KafkaApplicationConfig {

    @Value("${tabularasa.kafka.bootstrap-servers:kafka:9092}")
    private String bootstrapServers;

    @Value("${tabularasa.kafka.group-id:tabularasa-bi-group}")
    private String groupId;

    @Value("${tabularasa.kafka.topic.ad-events:ad-events-topic}")
    private String adEventsTopic;

    @Value("${tabularasa.kafka.topic.campaign-stats:campaign-stats-topic}")
    private String campaignStatsTopic;

    // ==== KAFKA PROPERTIES ====

    // General application properties can be added here
} 