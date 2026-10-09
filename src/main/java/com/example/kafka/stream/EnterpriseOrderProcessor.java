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
    private final String stateStore;

    public EnterpriseOrderProcessor(OrderProcessor orderProcessor, DlqService dlqService, String stateStore) {
        this.orderProcessor = orderProcessor;
        this.dlqService = dlqService;
        this.stateStore = stateStore;
    }

    @Override
    public void init(ProcessorContext<String, OrderRoutingResult> context) {
        super.init(context);
        this.retryStore = context.getStateStore(stateStore);
    }

    @Override
    public void process(Record<String, Order> record) {
        Order order = record.value();

        // Tombstone: non c'è nulla da elaborare.
        if (order == null) {
            log.warn("[PROCESSOR] Skipping record with null value: key={}", record.key());
            return;
        }

        // Senza orderId non si può usare lo state store (KeyValueStore.get(null) lancia NPE, ucciderebbe lo stream
        // thread a ogni riavvio, perché il record resterebbe l'offset da rielaborare): il messaggio non è
        // elaborabile e non lo sarà mai con un retry, quindi va direttamente in DLQ.
        String orderId = order.orderId();
        if (orderId == null || orderId.isBlank()) {
            log.error("[PROCESSOR] Order without orderId: key={}, productId={}. Forwarding to DLQ.", record.key(), order.productId());
            forwardToDlq(record, order, new IllegalArgumentException("Missing orderId"));
            return;
        }

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
            log.info("[PROCESSOR] Order with orderId {} processed successfully after {} failed attempts", orderId, attempts);
        }

        // Se va bene, inoltriamo contrassegnando come SUCCESS e puliamo lo store
        retryStore.delete(orderId);
        context().forward(record.withValue(new OrderRoutingResult(order, OrderRoutingResult.RoutingStatus.SUCCESS)));
    }

    private void handleProcessException(Record<String, Order> record, Exception e, String orderId, Order order) {
        Integer attempts = retryStore.get(orderId);
        if (attempts == null) attempts = 0;
        attempts++;

        if (attempts < MAX_RETRIES) {
            log.warn("[PROCESSOR] Error with orderId {}. Attempt {} of {}. Forwarding to Retry.", orderId, attempts, MAX_RETRIES);
            retryStore.put(orderId, attempts);
            context().forward(record.withValue(new OrderRoutingResult(order, OrderRoutingResult.RoutingStatus.RETRY)));
        } else {
            log.error("[PROCESSOR] Order with orderId {} failed definitively after {} attempts. Forwarding to DLQ.",
                    orderId, attempts);
            retryStore.delete(orderId); // Evita memory leak su RocksDB
            forwardToDlq(record, order, e);
        }
    }

    private void forwardToDlq(Record<String, Order> record, Order order, Exception cause) {
        String workerId = dlqService.getWorkerId(record.headers());
        Record<String, OrderRoutingResult> dlqRecord = dlqService
                .createDlqRecord(order, workerId, cause)
                .withValue(new OrderRoutingResult(order, OrderRoutingResult.RoutingStatus.DLQ));

        context().forward(dlqRecord);
    }
}
