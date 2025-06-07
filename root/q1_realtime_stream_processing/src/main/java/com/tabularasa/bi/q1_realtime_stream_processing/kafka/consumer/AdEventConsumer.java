package com.tabularasa.bi.q1_realtime_stream_processing.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

// Temporarily disabled to allow AdEventsSimpleProcessor to process messages
// @Service
public class AdEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AdEventConsumer.class);

    /**
     * Kafka consumer for ad events. Used for testing and demonstration.
     */
    // ==== KAFKA LISTENER ====
    /**
     * Listens to ad event messages from Kafka and logs them.
     *
     * @param record Kafka consumer record
     */
    @KafkaListener(topics = "${kafka.topic.ad-events}", groupId = "${kafka.group.id}")
    public void listen(ConsumerRecord<String, String> record) {
        log.info("Received message from Kafka topic '{}': Key='{}', Value='{}', Partition={}, Offset={}",
                record.topic(),
                record.key(), record.value(), record.partition(), record.offset());
    }
} 