package com.example.kafka.stream;

import com.example.kafka.model.Order;

public record OrderRoutingResult(Order order, RoutingStatus status) {
    public enum RoutingStatus {
        SUCCESS, RETRY, DLQ
    }
}

