package com.example.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Unico punto di pubblicazione sulla DLQ degli ordini.
 */
@Slf4j
@Component
public class DlqPublisher {

    public static final String WORKER_ID = "workerId";
    public static final String HEADER_EXCEPTION_CLASS = "dlq-exception-class";
    public static final String HEADER_EXCEPTION_MESSAGE = "dlq-exception-message";
    public static final String HEADER_EXCEPTION_STACKTRACE = "dlq-exception-stacktrace";
    private static final int MAX_FIELD_LENGTH = 64;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppKafkaProperties appProperties;

    public DlqPublisher(KafkaTemplate<String, Object> kafkaTemplate, AppKafkaProperties appProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.appProperties = appProperties;
    }

    /**
     * @param key       chiave originale del messaggio (verrà troncata se troppo lunga)
     * @param retry     descrizione del messaggio fallito
     * @param workerId  l'ID del worker che ha gestito il fallimento
     * @param cause     eccezione che ha causato l'invio in DLQ, se disponibile
     */
    public CompletableFuture<SendResult<String, Object>> publish(String key, Order retry, String workerId, Throwable cause) {

        String dlqTopic = appProperties.getDlqTopics().getOrders();
        Order payload = compact(retry);
        String orderId = Optional.ofNullable(payload).map(Order::orderId).orElse(null);
        String productId = Optional.ofNullable(payload).map(Order::productId).orElse(null);

        List<Header> headers = new ArrayList<>();
        headers.add(header(WORKER_ID, workerId));
        if (cause != null) {
            headers.add(header(HEADER_EXCEPTION_CLASS, cause.getClass().getName()));
            headers.add(header(HEADER_EXCEPTION_MESSAGE, cause.getMessage()));
            headers.add(header(HEADER_EXCEPTION_STACKTRACE, ExceptionUtils.getStackTrace(cause)));
        }

        ProducerRecord<String, Object> record = new ProducerRecord<>(dlqTopic, null, null, key, payload, headers);

        return kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[DLQ] Failed to send to {}. workerId={}, orderId={}, productId={}. Error: {}", dlqTopic, workerId, orderId, productId, ex.getMessage());
                    } else {
                        log.info("[DLQ] Sent to {}. workerId={}, orderId={}, productId={}", dlqTopic, workerId, orderId, productId);
                    }
                });
    }

    private static Order compact(Order order) {
        if (null == order) {
            return null;
        }
        return new Order(
                StringUtils.abbreviate(order.orderId(), MAX_FIELD_LENGTH),
                StringUtils.abbreviate(order.customerId(), MAX_FIELD_LENGTH),
                StringUtils.abbreviate(order.productId(), MAX_FIELD_LENGTH),
                order.quantity(),
                order.timestamp()
        );
    }

    private static Header header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
