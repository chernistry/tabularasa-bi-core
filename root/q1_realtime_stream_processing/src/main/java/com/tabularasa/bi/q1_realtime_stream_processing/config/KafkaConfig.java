package com.tabularasa.bi.q1_realtime_stream_processing.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for both Spark and simple profiles.
 */
@Configuration
public class KafkaConfig {

    // TODO: Configure SSL/TLS for secure communication with Kafka brokers in production.
    // Example properties:
    // security.protocol=SSL
    // ssl.truststore.location=/path/to/truststore.jks
    // ssl.truststore.password=...
    // ssl.keystore.location=/path/to/keystore.jks
    // ssl.keystore.password=...
    // TODO: Enforce SASL authentication (e.g., SASL_SSL with PLAIN or SCRAM) for production environments.
    // Example properties:
    // sasl.mechanism=SCRAM-SHA-512
    // sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="..." password="...";

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${app.kafka.consumer.group-id:bi-stream-processor}")
    private String groupId;
    
    @Value("${app.kafka.consumer.auto-offset-reset:latest}")
    private String autoOffsetReset;

    /**
     * Consumer factory for standard Spring Kafka consumers (simple profile).
     */
    @Bean
    @Profile("simple")
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }
} 