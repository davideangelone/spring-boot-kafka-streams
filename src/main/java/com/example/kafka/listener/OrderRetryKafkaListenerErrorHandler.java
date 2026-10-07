package com.example.kafka.listener;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.example.kafka.dlq.DlqPublisher;
import com.example.kafka.model.OrderRetry;
import com.example.kafka.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component("orderRetryKafkaListenerErrorHandler")
public class OrderRetryKafkaListenerErrorHandler implements KafkaListenerErrorHandler {

    private final DlqPublisher dlqPublisher;

    public OrderRetryKafkaListenerErrorHandler(DlqPublisher dlqPublisher) {
        this.dlqPublisher = dlqPublisher;
    }

    @Override
    public Object handleError(
            Message<?> message,
            @NonNull ListenerExecutionFailedException exception) {

        var record = (OrderRetry) message.getPayload();

        // La chiave può mancare: il messaggio va comunque in DLQ (senza chiave), non va perso
        String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
        if (key == null) {
            log.warn("[LISTENER-ERROR-HANDLER] Message without key, sending to DLQ anyway: orderId={}", record.order().orderId());
        }

        log.error(
                "[LISTENER-ERROR-HANDLER] Order retry processing failed after {} attempts. Sending to DLQ: orderId={}, productId={}, topic={}, partition={}, offset={}",
                OrderRetryAttemptCounter.get(record.order().orderId()),
                record.order().orderId(),
                record.order().productId(),
                record.originalTopic(),
                record.originalPartition(),
                record.originalOffset(),
                exception
        );

        OrderRetryAttemptCounter.reset(record.order().orderId());

        try {
            dlqPublisher.publish(key, record, "retry-listener", rootCause(exception))
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        } catch (CompletionException e) {
            // Se anche la DLQ fallisce l'eccezione risale al container, che ritenta la consegna: nulla viene perso in silenzio
            log.error("[LISTENER-ERROR-HANDLER] Failed to publish message to DLQ: [{}]", JsonUtils.toJson(record), e);
            throw e;
        }

        return record;
    }

    private static Throwable rootCause(Throwable throwable) {
        return NestedExceptionUtils.getMostSpecificCause(throwable);
    }
}
