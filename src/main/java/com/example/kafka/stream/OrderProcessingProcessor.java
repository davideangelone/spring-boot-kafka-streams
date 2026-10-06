package com.example.kafka.stream;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

@Slf4j
public class OrderProcessingProcessor extends ContextualProcessor<String, Order, String, OrderProcessingResult> {

    private final AtomicLong count = new AtomicLong(0);
    private final OrderProcessor orderProcessor;

    public OrderProcessingProcessor(OrderProcessor orderProcessor) {
        this.orderProcessor = orderProcessor;
    }

    @Override
    public void process(Record<String, Order> record) {

        Order order = record.value();

        try {
            logProcessing(order);
            orderProcessor.process(order);

            context().forward(record.withValue(OrderProcessingResult.success(order)));
        } catch (Exception e) {
            context().forward(record.withValue(OrderProcessingResult.failure(getOrderRetry(order, e))));
        }
    }

    private OrderRetry getOrderRetry(Order order, Exception e) {
        var metadata = context().recordMetadata();
        return new OrderRetry(
                UUID.randomUUID().toString(),
                order,
                metadata.map(RecordMetadata::topic).orElse(null),
                metadata.map(RecordMetadata::partition).orElse(-1),
                metadata.map(RecordMetadata::offset).orElse(-1L),
                System.currentTimeMillis(),
                e.getClass().getName(),
                e.getMessage()
        );
    }

    private void logProcessing(Order order) {

        long currentCount = count.incrementAndGet();
        if (log.isDebugEnabled() && (currentCount % 5000 == 0)) {

            var metadata = context().recordMetadata();

            String topic = metadata.map(RecordMetadata::topic).orElse(null);
            int partition = metadata.map(RecordMetadata::partition).orElse(-1);
            long offset = metadata.map(RecordMetadata::offset).orElse(-1L);
            long age = System.currentTimeMillis() - order.timestamp();

            log.debug(
                    "Processing order={}, task={}, thread={}, topic={}, partition={}, offset={}, age={} ms",
                    order.orderId(),
                    context().taskId(),
                    Thread.currentThread().getName(),
                    topic,
                    partition,
                    offset,
                    age
            );
        }
    }
}
