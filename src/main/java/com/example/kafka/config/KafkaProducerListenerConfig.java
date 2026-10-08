package com.example.kafka.config;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import com.example.kafka.dlq.DlqPublisher;
import com.example.kafka.model.Order;
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
    private final DlqPublisher dlqPublisher;
    private final AppKafkaProperties appProperties;

    public KafkaProducerListenerConfig(KafkaTemplate<String, Object> kafkaTemplate,
                                       DlqPublisher dlqPublisher,
                                       AppKafkaProperties appProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.dlqPublisher = dlqPublisher;
        this.appProperties = appProperties;
    }

    @PostConstruct
    public void registerListener() {
        // Agganciamo un ascoltatore degli esiti direttamente al template di Spring.
        // NB: il template è condiviso da tutti i producer dell'applicazione (orders, notifications, DLQ),
        // quindi qui si reagisce solo ai fallimenti sul topic degli ordini.
        kafkaTemplate.setProducerListener(new ProducerListener<>() {
            @Override
            public void onError(@NonNull ProducerRecord<String, Object> producerRecord,
                                RecordMetadata recordMetadata, @NonNull Exception exception) {

                String topic = producerRecord.topic();

                // Read workerId from headers if present
                Header workerHeader = producerRecord.headers().lastHeader("workerId");
                String workerId = Optional.ofNullable(workerHeader)
                        .map(Header::value)
                        .map(value -> new String(value, StandardCharsets.UTF_8))
                        .orElse("unknown");

                // Altri topic (notifications, DLQ, ...): il chiamante gestisce già l'errore.
                // In particolare un fallimento sulla DLQ non deve generare un nuovo invio in DLQ.
                if (!topic.equals(appProperties.getTopics().getOrders())
                        || !(producerRecord.value() instanceof Order order)) {
                    log.error("[WorkerId {}] Send failed on topic {}. Error: {}", workerId, topic, exception.getMessage());
                    return;
                }

                String dlqTopic = appProperties.getDlqTopics().getOrders();
                log.error("[WorkerId {}] Send failed on topic {}. Redirecting to {}. orderId={}, productId={}. Error: {}",
                        workerId, topic, dlqTopic, order.orderId(), order.productId(), exception.getMessage());

                dlqPublisher.publish(producerRecord.key(), order, workerId, null);
            }
        });
    }
}
