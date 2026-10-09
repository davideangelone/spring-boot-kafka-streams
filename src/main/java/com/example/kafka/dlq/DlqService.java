package com.example.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import com.example.kafka.model.Order;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Service;

import static com.example.kafka.constants.Headers.HEADER_EXCEPTION_CLASS;
import static com.example.kafka.constants.Headers.HEADER_EXCEPTION_MESSAGE;
import static com.example.kafka.constants.Headers.UNKNOWN_WORKER_ID;
import static com.example.kafka.constants.Headers.WORKER_ID;

/**
 * Costruisce i record da scrivere in DLQ. Lo usano sia la topologia Streams sia {@link DlqPublisher}, così i
 * messaggi in DLQ hanno sempre lo stesso schema: chiave = orderId, valore = {@link Order} compattato e header diagnostici.
 * <p>
 * Tutto ciò che finisce nel record è limitato in dimensione: un messaggio scartato perché troppo grande verrebbe
 * rifiutato anche dalla DLQ (stesso producer, stessi limiti) e andrebbe perso.
 */
@Service
public class DlqService {

    static final int MAX_FIELD_LENGTH = 64;
    static final int MAX_MESSAGE_LENGTH = 200;

    public Record<String, Order> createDlqRecord(Order order, String workerId, Exception cause) {
        Objects.requireNonNull(order, "order must not be null");
        Order payload = compact(order);

        // Il timestamp del record Kafka non può essere negativo (Record lancia una StreamsException): un ordine
        // con timestamp non valido deve comunque arrivare in DLQ. Il valore originale resta nel payload.
        long timestamp = Math.max(0L, payload.timestamp());

        Record<String, Order> result = new Record<>(payload.orderId(), payload, timestamp);
        result.headers().add(header(WORKER_ID, Optional.ofNullable(workerId).orElse(UNKNOWN_WORKER_ID)));
        if (cause != null) {
            result.headers().add(header(HEADER_EXCEPTION_CLASS, cause.getClass().getName()));
            result.headers().add(header(HEADER_EXCEPTION_MESSAGE, StringUtils.abbreviate(cause.getMessage(), MAX_MESSAGE_LENGTH)));
        }
        return result;
    }

    public String getWorkerId(Headers headers) {
        return Optional.ofNullable(headers.lastHeader(WORKER_ID))
                .map(Header::value)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElse(UNKNOWN_WORKER_ID);
    }

    private static Order compact(Order order) {
        return new Order(
                order.orderId(),
                StringUtils.abbreviate(order.customerId(), MAX_FIELD_LENGTH),
                StringUtils.abbreviate(order.productId(), MAX_FIELD_LENGTH),
                order.quantity(),
                order.timestamp()
        );
    }

    private static Header header(String name, String value) {
        return new RecordHeader(name, Optional.ofNullable(value).orElse("").getBytes(StandardCharsets.UTF_8));
    }
}
