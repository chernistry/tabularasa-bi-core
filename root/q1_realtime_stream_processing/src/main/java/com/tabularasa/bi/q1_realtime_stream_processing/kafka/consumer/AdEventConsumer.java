package com.tabularasa.bi.q1_realtime_stream_processing.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Kafka consumer for ad events. Used for testing and demonstration.
 * Temporarily disabled to allow AdEventsSimpleProcessor to process messages.
 */
// @Service
public class AdEventConsumer {

    /**
     * Logger for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AdEventConsumer.class);

    /**
     * Listens to ad event messages from Kafka and logs them.
     *
     * @param record Kafka consumer record.
     */
    @KafkaListener(topics = "${kafka.topic.ad-events}", groupId = "${kafka.group.id}")
    public void listen(final ConsumerRecord<String, String> record) {
        LOG.info("Received message from Kafka topic '{}': Key='{}', Value='{}', "
                + "Partition={}, Offset={}",
                record.topic(),
                record.key(), record.value(), record.partition(), record.offset());
    }
}