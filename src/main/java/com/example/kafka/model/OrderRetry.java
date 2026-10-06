package com.example.kafka.model;

public record OrderRetry (
        String eventId,
        Order order,
        String originalTopic,
        int originalPartition,
        long originalOffset,
        long failedAt,
        String errorType,
        String errorMessage
) {
}