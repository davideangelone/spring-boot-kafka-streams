package com.example.kafka.listener;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.OrderRetry;
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

        String orderId = retry.order().orderId();
        int attempt = OrderRetryAttemptCounter.next(orderId);
        String identifier = "[PROCESSOR-LISTENER-RETRY #" + attempt + "]";

        log.info("{} Retry processing: orderId={}", identifier, orderId);

        // Può lanciare un'eccezione (simulata): in tal caso scatta @Retryable e, a tentativi esauriti, l'error handler
        orderProcessor.processRetry(retry);

        // Si pubblica lo stesso tipo (Order) che scrive la topologia Streams, così su notifications-topic
        // esiste un solo schema. L'invio viene atteso: se fallisce l'eccezione risale al listener
        // (retry/DLQ) invece di perdersi in un callback asincrono con l'offset già committato.
        try {
            kafkaTemplate.send(appProperties.getTopics().getNotifications(), orderId, retry.order())
                    .orTimeout(5, TimeUnit.SECONDS)
                    .join();
        } catch (CompletionException e) {
            log.error("{} Failed to publish message orderId={} to notifications topic: [{}]",
                    identifier, orderId, JsonUtils.toJson(retry.order()), e);
            throw e;
        }

        log.info("{} Retry processing successful: Published message orderId={} to notifications topic", identifier, orderId);

        OrderRetryAttemptCounter.reset(orderId);
    }
}
