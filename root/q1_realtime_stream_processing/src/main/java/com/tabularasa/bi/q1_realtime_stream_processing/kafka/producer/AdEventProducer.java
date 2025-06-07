package com.tabularasa.bi.q1_realtime_stream_processing.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka producer for ad events.
 *
 * <p>Sends ad event messages to the configured Kafka topic.
 */
@Service
public class AdEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(AdEventProducer.class);

    @Value("${kafka.topic.ad_events:ad_events_topic}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public AdEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends an ad event message to the configured ad-events Kafka topic.
     * The message is currently a String, but could be a JSON representation of an AdEvent POJO.
     *
     * @param eventPayload The ad event message string.
     */
    public void sendAdEvent(String eventPayload) {
        logger.info("Sending ad event: {}", eventPayload);
        kafkaTemplate.send(topicName, eventPayload);
    }

    /**
     * Sends an ad event message to the configured ad-events Kafka topic with a specific key.
     *
     * @param key The key for the Kafka message (e.g., campaign_id or user_id for partitioning).
     * @param eventPayload The ad event message string.
     */
    public void sendAdEvent(String key, String eventPayload) {
        logger.info("Sending ad event with key {}: {}", key, eventPayload);
        kafkaTemplate.send(topicName, key, eventPayload);
    }

    /*
    // Example for sending an AdEvent POJO:
    public void sendAdEvent(AdEvent adEvent) {
        try {
            log.info("Sending adEvent: {} to topic: {}", adEvent, kafkaApplicationConfig.getAdEventsTopic());
            // Assuming you have a kafkaTemplate for AdEvent objects
            // this.adEventKafkaTemplate.send(kafkaApplicationConfig.getAdEventsTopic(), adEvent);
        } catch (Exception e) {
            log.error("Error sending adEvent to Kafka: {}", adEvent, e);
        }
    }
    */
} 