package com.example.kafka.stream;

import java.util.concurrent.ThreadLocalRandom;

import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderProcessor {

    private final double errorRate;
    private final int errorTimestampModulo;
    private final int errorTimestampThreshold;

    public OrderProcessor(@Value("${load-generator.error-rate}") double errorRate,
                          @Value("${load-generator.error-timestamp-modulo}") int errorTimestampModulo,
                          @Value("${load-generator.error-timestamp-threshold}") int errorTimestampThreshold) {
        this.errorRate = errorRate;
        this.errorTimestampModulo = errorTimestampModulo;
        this.errorTimestampThreshold = errorTimestampThreshold;
    }

    public void process(Order order) {
        log.debug("[PROCESSOR] Processing order={}, product={}", order.orderId(), order.productId());

        // Simula errori di processamento basato su timestamp (falliscono tutti i tentativi)
        if (order.timestamp() % errorTimestampModulo < errorTimestampThreshold) {
            log.debug("[PROCESSOR] Simulated processing error (timestamp): orderId={}, productId={}", order.orderId(), order.productId());
            throw new RuntimeException("[PROCESSOR] Simulated processing error (timestamp)");
        }

        // Simula errori di processamento randomici
        if (ThreadLocalRandom.current().nextDouble() < errorRate) {
            log.debug("[PROCESSOR] Simulated processing error (random): orderId={}, productId={}", order.orderId(), order.productId());
            throw new RuntimeException("[PROCESSOR] Simulated processing error (random)");
        }
    }
}
