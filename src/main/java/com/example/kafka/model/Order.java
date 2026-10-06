package com.example.kafka.model;

public record Order(
        String orderId,
        String customerId,
        String productId,
        int quantity,
        long timestamp
) {
}