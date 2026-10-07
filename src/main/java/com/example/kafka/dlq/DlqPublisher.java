package com.example.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Unico punto di pubblicazione sulla DLQ degli ordini.
 * <p>
 * Tutti i messaggi in DLQ hanno lo stesso schema ({@link OrderRetry}) e gli stessi header, qualunque sia
 * il punto in cui il fallimento è avvenuto (producer, listener di retry). Il payload viene "compattato":
 * i campi di testo sono troncati, altrimenti un messaggio scartato perché troppo grande verrebbe
 * rifiutato anche dalla DLQ (stesso producer, stessi limiti) e andrebbe perso.
 */
@Slf4j
@Component
public class DlqPublisher {

    public static final String HEADER_SOURCE = "dlq-source";
    public static final String HEADER_EXCEPTION_CLASS = "dlq-exception-class";
    public static final String HEADER_EXCEPTION_MESSAGE = "dlq-exception-message";

    static final int MAX_FIELD_LENGTH = 64;
    static final int MAX_MESSAGE_LENGTH = 200;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppKafkaProperties appProperties;

    public DlqPublisher(KafkaTemplate<String, Object> kafkaTemplate, AppKafkaProperties appProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.appProperties = appProperties;
    }

    /**
     * @param key    chiave originale del messaggio (verrà troncata se troppo lunga)
     * @param retry  descrizione del messaggio fallito
     * @param source da dove arriva il fallimento (es. "producer", "retry-listener")
     * @param cause  eccezione che ha causato l'invio in DLQ, se disponibile
     */
    public CompletableFuture<SendResult<String, Object>> publish(String key, OrderRetry retry, String source, Throwable cause) {

        String dlqTopic = appProperties.getDlqTopics().getOrders();
        OrderRetry payload = compact(retry);
        String orderId = payload.order() != null ? payload.order().orderId() : null;
        String productId = payload.order() != null ? payload.order().productId() : null;

        List<Header> headers = new ArrayList<>();
        headers.add(header(HEADER_SOURCE, source));
        if (cause != null) {
            headers.add(header(HEADER_EXCEPTION_CLASS, cause.getClass().getName()));
            headers.add(header(HEADER_EXCEPTION_MESSAGE, truncate(cause.getMessage(), MAX_MESSAGE_LENGTH)));
        }

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(dlqTopic, null, null, truncate(key, MAX_FIELD_LENGTH), payload, headers);

        return kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[DLQ] Failed to send to {}. source={}, orderId={}, productId={}. Error: {}", dlqTopic, source, orderId, productId, ex.getMessage());
                    } else {
                        log.info("[DLQ] Sent to {}. source={}, orderId={}, productId={}", dlqTopic, source, orderId, productId);
                    }
                });
    }

    private static OrderRetry compact(OrderRetry retry) {
        Order order = retry.order();
        Order compactOrder = order == null ? null : new Order(
                truncate(order.orderId(), MAX_FIELD_LENGTH),
                truncate(order.customerId(), MAX_FIELD_LENGTH),
                truncate(order.productId(), MAX_FIELD_LENGTH),
                order.quantity(),
                order.timestamp()
        );
        return new OrderRetry(
                retry.eventId(),
                compactOrder,
                retry.originalTopic(),
                retry.originalPartition(),
                retry.originalOffset(),
                retry.failedAt(),
                retry.errorType(),
                truncate(retry.errorMessage(), MAX_MESSAGE_LENGTH)
        );
    }

    static String truncate(String value, int max) {
        if (value == null || value.length() <= max) {
            return value;
        }
        return value.substring(0, max - 3) + "...";
    }

    private static Header header(String name, String value) {
        return new RecordHeader(name, (value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
    }
}
