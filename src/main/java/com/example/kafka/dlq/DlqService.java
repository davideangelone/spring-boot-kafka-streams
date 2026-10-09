package com.example.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.example.kafka.model.Order;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Service;

@Service
public class DlqService {

    private static final String WORKER_ID = "workerId";
    private static final String HEADER_EXCEPTION_CLASS = "dlq-exception-class";
    private static final String HEADER_EXCEPTION_MESSAGE = "dlq-exception-message";
    private static final int MAX_FIELD_LENGTH = 64;

    public Record<String, Order> createDlqRecord(Order order, String workerId, Exception cause) {
        Order payload = compact(order);
        String orderId = Optional.ofNullable(payload).map(Order::orderId).orElse(null);

        List<Header> headers = new ArrayList<>();
        headers.add(header(WORKER_ID, workerId));
        if (cause != null) {
            headers.add(header(HEADER_EXCEPTION_CLASS, cause.getClass().getName()));
            headers.add(header(HEADER_EXCEPTION_MESSAGE, cause.getMessage()));
        }

        Record<String, Order> result = new Record<>(orderId, payload, payload.timestamp());
        headers.forEach(header -> result.headers().add(header));
        return result;
    }

    public String getWorkerId(Headers headers) {
        return Optional.ofNullable(headers.lastHeader(WORKER_ID))
                .map(Header::value)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .orElse(null);
    }

    private static Order compact(Order order) {
        if (null == order) {
            return null;
        }
        return new Order(
                order.orderId(),
                StringUtils.abbreviate(order.customerId(), MAX_FIELD_LENGTH),
                StringUtils.abbreviate(order.productId(), MAX_FIELD_LENGTH),
                order.quantity(),
                order.timestamp()
        );
    }

    private static Header header(String name, String value) {
        if (null == value) {
            value = "";
        }
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
