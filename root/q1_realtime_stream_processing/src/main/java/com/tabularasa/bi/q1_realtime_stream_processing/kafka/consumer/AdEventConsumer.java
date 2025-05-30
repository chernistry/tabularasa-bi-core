package com.tabularasa.bi.q1_realtime_stream_processing.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AdEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AdEventConsumer.class);

    // This consumer is primarily for testing/demonstration purposes.
    // It listens to the topic defined by ${kafka.topic.ad-events}
    // and logs the received messages.
    @KafkaListener(topics = "${kafka.topic.ad-events}", groupId = "${kafka.group.id}")
    public void listen(ConsumerRecord<String, String> record) {
        log.info("Received message from Kafka topic '{}': Key='{}', Value='{}', Partition={}, Offset={}",
                record.topic(),
                record.key(), record.value(), record.partition(), record.offset());
    }
} 