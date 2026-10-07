package com.example.kafka.listener;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.OrderRetry;
import com.example.kafka.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component("orderRetryKafkaListenerErrorHandler")
public class OrderRetryKafkaListenerErrorHandler implements KafkaListenerErrorHandler {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppKafkaProperties appProperties;

    public OrderRetryKafkaListenerErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            AppKafkaProperties appProperties) {

        this.kafkaTemplate = kafkaTemplate;
        this.appProperties = appProperties;
    }

    @Override
    public Object handleError(
            Message<?> message,
            @NonNull ListenerExecutionFailedException exception) {

        var record = (OrderRetry) message.getPayload();
        String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_KEY);

        if (null == key) {
            log.error("[LISTENER-ERROR-HANDLER] Failed to retrieve key from message headers: {}", message.getHeaders());
            return record;
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

        String dlqTopic = appProperties.getDlqTopics().getOrders();

        kafkaTemplate
                .send(dlqTopic, key, message.getPayload())
                .orTimeout(5, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    String msg = "[LISTENER-ERROR-HANDLER] Failed to publish message to DLQ: [" + JsonUtils.toJson(record) + "]";
                    log.error(msg, ex);
                    throw new CompletionException("[LISTENER-ERROR-HANDLER] Failed to publish message to DLQ", ex);
                })
                .join();

        return record;
    }
}
