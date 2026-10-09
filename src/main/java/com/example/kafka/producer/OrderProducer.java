package com.example.kafka.producer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import static com.example.kafka.constants.Headers.WORKER_ID;

@Slf4j
@Component
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final AppKafkaProperties appProperties;
    private final double errorRate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate, AppKafkaProperties appProperties, @Value("${load-generator.error-rate}") double errorRate) {
        this.kafkaTemplate = kafkaTemplate;
        this.appProperties = appProperties;
        this.errorRate = errorRate;
    }

    public CompletableFuture<SendResult<String, Order>> send(Order order, int workerId) {
        if (ThreadLocalRandom.current().nextDouble() < errorRate) {
            log.error("[WorkerId {}] Simulated producer error: orderId={}, productId={}", workerId, order.orderId(), order.productId());
            order = new Order(
                    order.orderId(),
                    order.customerId(),
                    "X".repeat(10000), // Simulate a large payload beyond configured limits
                    order.quantity(),
                    order.timestamp()
            );
        }

        ProducerRecord<String, Order> record = new ProducerRecord<>(
                appProperties.getTopics().getOrders(),
                order.orderId(),
                order
        );
        record.headers().add(WORKER_ID, String.valueOf(workerId).getBytes(StandardCharsets.UTF_8));

        return kafkaTemplate.send(record);
    }
}
