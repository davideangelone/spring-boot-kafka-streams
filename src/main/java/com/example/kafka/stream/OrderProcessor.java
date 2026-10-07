package com.example.kafka.stream;

import java.util.concurrent.ThreadLocalRandom;

import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderProcessor {

    private final double errorRate;
    private final double retryErrorRate;

    public OrderProcessor(
            @Value("${load-generator.error-rate}") double errorRate,
            @Value("${load-generator.retry-error-rate:${load-generator.error-rate}}") double retryErrorRate) {
        this.errorRate = errorRate;
        this.retryErrorRate = retryErrorRate;
    }

    public void process(Order order) {
        log.debug("[PROCESSOR] Processing order={}, product={}", order.orderId(), order.productId());

        if (ThreadLocalRandom.current().nextDouble() < errorRate) {
            log.error("[PROCESSOR] Simulated processing error: orderId={}, productId={}", order.orderId(), order.productId());
            throw new RuntimeException("[PROCESSOR] Simulated processing error");
        }
    }

    public void processRetry(OrderRetry orderRetry) {
        log.debug("[PROCESSOR-RETRY] Processing order={}, product={}", orderRetry.order().orderId(), orderRetry.order().productId());

        if (ThreadLocalRandom.current().nextDouble() < retryErrorRate) {
            log.error("[PROCESSOR-RETRY] Simulated retry processing error: orderId={}, productId={}", orderRetry.order().orderId(), orderRetry.order().productId());
            throw new RuntimeException("[PROCESSOR-RETRY] Simulated retry processing error");
        }
    }
}
