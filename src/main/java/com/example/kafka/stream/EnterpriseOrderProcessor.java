package com.example.kafka.stream;

import com.example.kafka.dlq.DlqService;
import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class EnterpriseOrderProcessor extends ContextualProcessor<String, Order, String, OrderRoutingResult> {

    private static final int MAX_RETRIES = 3;
    private KeyValueStore<String, Integer> retryStore;
    private final OrderProcessor orderProcessor;
    private final DlqService dlqService;

    public EnterpriseOrderProcessor(OrderProcessor orderProcessor, DlqService dlqService) {
        this.orderProcessor = orderProcessor;
        this.dlqService = dlqService;
    }

    @Override
    public void init(ProcessorContext<String, OrderRoutingResult> context) {
        super.init(context);
        this.retryStore = context.getStateStore("order-retry-counts");
    }

    @Override
    public void process(Record<String, Order> record) {
        Order order = record.value();
        String orderId = order.orderId();

        try {
            // Esegui la logica che può fallire
            orderProcessor.process(order);
        } catch (Exception e) {
            handleProcessException(record, e, orderId, order);
            return;
        }

        handleProcessSuccess(record, orderId, order);
    }

    private void handleProcessSuccess(Record<String, Order> record, String orderId, Order order) {
        Integer attempts = retryStore.get(orderId);
        if ((null != attempts) && attempts > 0) {
            log.info("[PROCESSOR] Order with orderId {} processed successfully after {} attempts", orderId, attempts);
        }

        // Se va bene, inoltriamo contrassegnando come SUCCESS e puliamo lo store
        retryStore.delete(orderId);
        context().forward(record.withValue(new OrderRoutingResult(order, OrderRoutingResult.RoutingStatus.SUCCESS)));
    }

    private void handleProcessException(Record<String, Order> record, Exception e, String orderId, Order order) {
        Integer attempts = retryStore.get(orderId);
        if (attempts == null) attempts = 0;
        attempts++;

        if (attempts <= MAX_RETRIES) {
            log.warn("[PROCESSOR] Error with orderId {}. Attempt {} of {}. Forwarding to Retry.", orderId, attempts, MAX_RETRIES);
            retryStore.put(orderId, attempts);
            context().forward(record.withValue(new OrderRoutingResult(order, OrderRoutingResult.RoutingStatus.RETRY)));
        } else {
            log.error("[PROCESSOR] Order with orderId {} failed definitively after {} attempts. Forwarding to DLQ.", orderId, MAX_RETRIES);
            retryStore.delete(orderId); // Evita memory leak su RocksDB

            String workerId = dlqService.getWorkerId(record.headers());
            Record<String, OrderRoutingResult> dlqRecord = dlqService
                    .createDlqRecord(order, workerId, e)
                    .withValue(new OrderRoutingResult(order, OrderRoutingResult.RoutingStatus.DLQ));

            context().forward(dlqRecord);
        }
    }
}
