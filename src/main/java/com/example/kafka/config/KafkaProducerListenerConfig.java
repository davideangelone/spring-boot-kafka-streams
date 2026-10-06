package com.example.kafka.config;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.example.kafka.model.Order;
import com.example.kafka.util.TopicUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

@Configuration
@Slf4j
public class KafkaProducerListenerConfig {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerListenerConfig(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void registerListener() {
        // Agganciamo un ascoltatore degli esiti direttamente al template di Spring
        kafkaTemplate.setProducerListener(new ProducerListener<>() {
            @Override
            public void onError(@NonNull ProducerRecord<String, Object> producerRecord,
                                RecordMetadata recordMetadata, @NonNull Exception exception) {

                String originalTopic = producerRecord.topic();
                Order order = (Order) producerRecord.value();

                // Read workerId from headers if present
                Header workerHeader = producerRecord.headers().lastHeader("workerId");
                String workerId = Optional.ofNullable(workerHeader)
                        .map(Header::value)
                        .map(value -> new String(value, StandardCharsets.UTF_8))
                        .orElse("unknown");

                // Guard against recursive DLQ sends
                if (TopicUtils.isDlqTopic(originalTopic)) {
                    log.error("[WorkerId {}] Final failure: could not forward to DLQ topic {}. Giving up. OrderId {}. Error: {}",
                            workerId, originalTopic, order.orderId(), exception.getMessage());
                    return;
                }

                String dlqTopic = TopicUtils.mapTopicToDlq(originalTopic);

                log.error("[WorkerId {}] Send failed on topic {}. Redirecting to {}. OrderId {}. Error: {}",
                        workerId, originalTopic, dlqTopic, order.orderId(), exception.getMessage());

                // DLQ send with proper error handling
                sendToDlq(dlqTopic, producerRecord, workerId, order.orderId());
            }
        });
    }

    private void sendToDlq(String dlqTopic, ProducerRecord<String, Object> producerRecord, String workerId, String orderId) {

        // Preserve headers when creating the DLQ record
        ProducerRecord<String, Object> dlqRecord = new ProducerRecord<>(
                dlqTopic,
                null, // partition: let broker assign
                producerRecord.timestamp(),
                producerRecord.key(),
                producerRecord.value(),
                producerRecord.headers() // copy original headers
        );

        kafkaTemplate.send(dlqRecord)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[WorkerId {}] Failed to send to DLQ topic {}. OrderId {}. Error: {}", workerId, dlqTopic, orderId, ex.getMessage());
                    } else {
                        log.info("[WorkerId {}] Successfully sent to DLQ topic {}. OrderId {}.", workerId, dlqTopic, orderId);
                    }
                });
    }
}
