package com.example.kafka.listener;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.OrderRetry;
import com.example.kafka.stream.OrderProcessingResult;
import com.example.kafka.stream.OrderProcessor;
import com.example.kafka.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.resilience.annotation.Retryable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderRetryListener {

    private final OrderProcessor orderProcessor;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppKafkaProperties appProperties;

    public OrderRetryListener(OrderProcessor orderProcessor, KafkaTemplate<String, Object> kafkaTemplate, AppKafkaProperties appProperties) {
        this.orderProcessor = orderProcessor;
        this.kafkaTemplate = kafkaTemplate;
        this.appProperties = appProperties;
    }

    @Retryable(
            includes = {Exception.class},
            maxRetries = 2,
            delayString = "500ms",
            multiplier = 1.5,
            jitter = 100
    )
    @KafkaListener(
            topics = "${app.retry-topics.orders}",
            groupId = "orders-retry-processing-group",
            errorHandler = "orderRetryKafkaListenerErrorHandler"
    )
    public void process(OrderRetry retry) {

        int attempt = OrderRetryAttemptCounter.next(retry.order().orderId());
        String identifier = "[PROCESSOR-LISTENER-RETRY #" + attempt + "]";

        log.info("{} Retry processing: orderId={}", identifier, retry.order().orderId());

        orderProcessor.processRetry(retry);

        if (retry.order().timestamp() % 2 == 0) {
            log.error("{} Simulated retry processing error: orderId={}, productId={}", identifier, retry.order().orderId(), retry.order().productId());
            throw new RuntimeException(identifier + " Simulated retry processing error");
        }

        kafkaTemplate.send(
                        appProperties.getTopics().getNotifications(),
                        retry.order().orderId(),
                        OrderProcessingResult.success(retry.order())
                )
                .orTimeout(5, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    String msg = "{} Failed to publish message orderId={} to notifications topic: [{}]";
                    log.error(msg, identifier, retry.order().orderId(), JsonUtils.toJson(retry.order()), ex);
                    throw new CompletionException(identifier + " Failed to publish message orderId={" + retry.order().orderId() + "} to notifications topic", ex);
                })
                .thenAccept(
                        result -> log.info("{} Retry processing successful: Published message orderId={} to notifications topic", identifier, retry.order().orderId())
                );

        OrderRetryAttemptCounter.reset(retry.order().orderId());
    }
}