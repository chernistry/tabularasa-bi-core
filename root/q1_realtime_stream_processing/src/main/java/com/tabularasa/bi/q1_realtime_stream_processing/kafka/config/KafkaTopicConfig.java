package com.tabularasa.bi.q1_realtime_stream_processing.kafka.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Kafka topic configuration for ad events and campaign stats.
 */
@Configuration
public class KafkaTopicConfig {

    /**
     * The bootstrap servers for Kafka.
     */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * The name of the ad events topic.
     */
    @Value("${kafka.topic.ad_events:ad_events_topic}")
    private String adEventsTopicName;

    /**
     * The number of partitions for the ad events topic.
     */
    @Value("${kafka.topic.partitions:1}")
    private int adEventsTopicPartitions;

    /**
     * The replication factor for the ad events topic.
     */
    @Value("${kafka.topic.replication-factor:1}")
    private short adEventsTopicReplicationFactor;

    /**
     * The name of the campaign stats topic.
     */
    @Value("${kafka.topic.campaign-stats:campaign-stats-topic}")
    private String campaignStatsTopicName;

    /**
     * Creates a KafkaAdmin bean for topic management.
     *
     * @return KafkaAdmin instance.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * Defines the ad events topic.
     *
     * @return NewTopic instance.
     */
    @Bean
    public NewTopic adEventsTopic() {
        return new NewTopic(adEventsTopicName, adEventsTopicPartitions,
                adEventsTopicReplicationFactor);
        // For more control, you can use TopicBuilder:
        // return TopicBuilder.name(adEventsTopicName)
        // .partitions(adEventsTopicPartitions)
        // .replicas(adEventsTopicReplicationFactor)
        // .compact()
        // .build();
    }

    /**
     * Defines the campaign stats topic.
     *
     * @return NewTopic instance.
     */
    @Bean
    public NewTopic campaignStatsTopic() {
        return new NewTopic(campaignStatsTopicName, 1, (short) 1);
    }
}