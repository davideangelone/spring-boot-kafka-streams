package com.example.kafka.dlq;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Unico punto di pubblicazione sulla DLQ degli ordini.
 */
@Slf4j
@Component
public class DlqPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppKafkaProperties appProperties;
    private final DlqService dlqService;

    public DlqPublisher(KafkaTemplate<String, Object> kafkaTemplate, AppKafkaProperties appProperties, DlqService dlqService) {
        this.kafkaTemplate = kafkaTemplate;
        this.appProperties = appProperties;
        this.dlqService = dlqService;
    }

    /**
     * @param retry    descrizione del messaggio fallito
     * @param workerId l'ID del worker che ha gestito il fallimento
     * @param cause    eccezione che ha causato l'invio in DLQ, se disponibile
     */
    public CompletableFuture<SendResult<String, Object>> publish(Order retry, String workerId, Exception cause) {

        String dlqTopic = appProperties.getDlqTopics().getOrders();
        Record<String, Order> record = dlqService.createDlqRecord(retry, workerId, cause);
        String orderId = record.key();
        String productId = Optional.ofNullable(record.value()).map(Order::productId).orElse(null);

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(dlqTopic, null, record.timestamp(), orderId, record.value(), record.headers());

        return kafkaTemplate.send(producerRecord)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[DLQ] Failed to send to {}. workerId={}, orderId={}, productId={}. Error: {}", dlqTopic, workerId, orderId, productId, ex.getMessage());
                    } else {
                        log.info("[DLQ] Sent to {}. workerId={}, orderId={}, productId={}", dlqTopic, workerId, orderId, productId);
                    }
                });
    }
}
