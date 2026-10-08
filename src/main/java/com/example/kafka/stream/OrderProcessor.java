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

    public OrderProcessor(@Value("${load-generator.error-rate}") double errorRate) {
        this.errorRate = errorRate;
    }

    public void process(Order order) {
        log.debug("[PROCESSOR] Processing order={}, product={}", order.orderId(), order.productId());

        if ( (order.timestamp() % 1000 < 2) || (ThreadLocalRandom.current().nextDouble() < errorRate) ) {
            log.debug("[PROCESSOR] Simulated processing error: orderId={}, productId={}", order.orderId(), order.productId());
            throw new RuntimeException("[PROCESSOR] Simulated processing error");
        }
    }
}
