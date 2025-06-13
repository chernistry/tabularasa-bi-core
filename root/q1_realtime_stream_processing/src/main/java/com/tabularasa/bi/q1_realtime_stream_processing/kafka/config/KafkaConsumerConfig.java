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
        // Spark will manage offsets, so auto-commit should be handled carefully or disabled
        // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Or "latest"
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
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "_spark"); // Separate group for Spark
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Let Spark manage offsets
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Define behavior when no GCloud offset is found or current offset does not exist on server
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaParams;
    }
}