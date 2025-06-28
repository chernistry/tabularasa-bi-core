package com.tabularasa.bi.q1_realtime_stream_processing.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration for both Spring Kafka and Spark Streaming.
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:q1_stream_consumer_group}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.fetch-max-bytes:5242880}")
    private int fetchMaxBytes; // 5 MB default

    @Value("${spring.kafka.consumer.max-poll-records:500}")
    private int maxPollRecords;

    // ==== SPRING KAFKA CONSUMER FACTORY ====
    /**
     * Creates a ConsumerFactory for Spring Kafka listeners.
     *
     * @return ConsumerFactory instance
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Optimize settings for reliability and performance
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        // Settings to improve message retrieval reliability
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // 10 seconds
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ==== SPRING KAFKA LISTENER CONTAINER FACTORY ====
    /**
     * Creates a KafkaListenerContainerFactory for Spring Kafka listeners.
     *
     * @return KafkaListenerContainerFactory instance
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2); // Set the number of consumer threads
        factory.getContainerProperties().setPollTimeout(3000); // 3 seconds for polling
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setAutoStartup(true);
        return factory;
    }

    // ==== SPARK KAFKA CONSUMER PROPERTIES ====
    /**
     * Provides Kafka consumer properties for Spark Streaming.
     *
     * @return Map of Kafka consumer properties
     */
    @Bean(name = "sparkKafkaConsumerProperties")
    public Map<String, String> sparkKafkaConsumerProperties() {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("kafka.bootstrap.servers", bootstrapServers);
        kafkaParams.put("subscribe", "ad-events");
        kafkaParams.put("startingOffsets", "latest");
        kafkaParams.put("failOnDataLoss", "false");
        kafkaParams.put("maxOffsetsPerTrigger", "5000");
        
        // Use correct parameter names for Spark Structured Streaming
        kafkaParams.put("kafka.fetch.max.bytes", String.valueOf(fetchMaxBytes));
        kafkaParams.put("kafka.max.partition.fetch.bytes", String.valueOf(fetchMaxBytes));
        
        // These parameters are needed for Spark DStream API, but not for Structured Streaming
        // Keep them for backward compatibility with DStream API
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", groupId + "_spark");
        kafkaParams.put("auto.offset.reset", autoOffsetReset);
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("max.poll.records", String.valueOf(maxPollRecords));
        
        return kafkaParams;
    }
    
    /**
     * Provides parameters for Spark Structured Streaming Kafka Source
     * 
     * @return Map with parameters for use in Spark Structured Streaming
     */
    @Bean(name = "sparkStructuredStreamingKafkaOptions")
    public Map<String, String> sparkStructuredStreamingKafkaOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("kafka.bootstrap.servers", bootstrapServers);
        options.put("subscribe", "ad-events");
        options.put("startingOffsets", "latest");
        options.put("failOnDataLoss", "false");
        options.put("maxOffsetsPerTrigger", "5000");
        options.put("kafka.fetch.max.bytes", String.valueOf(fetchMaxBytes));
        options.put("kafka.max.partition.fetch.bytes", String.valueOf(fetchMaxBytes));
        return options;
    }
}