package com.example.kafka.stream;

import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;

public record OrderProcessingResult(
        Order order,
        OrderRetry retry
) {

    public static OrderProcessingResult success(Order order) {
        return new OrderProcessingResult(order, null);
    }

    public static OrderProcessingResult failure(OrderRetry retry) {
        return new OrderProcessingResult(null, retry);
    }

    public boolean isSuccess() {
        return order != null;
    }
}
