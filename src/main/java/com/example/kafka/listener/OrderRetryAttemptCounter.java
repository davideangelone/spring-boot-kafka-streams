package com.example.kafka.listener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class OrderRetryAttemptCounter {
    private static final ConcurrentMap<String, AtomicInteger> COUNTS = new ConcurrentHashMap<>();

    private OrderRetryAttemptCounter() {
    }

    public static int next(String orderId) {
        return COUNTS
                .compute(orderId, (k, v) -> v != null ? v : new AtomicInteger(0))
                .incrementAndGet();
    }

    public static int get(String orderId) {
        return COUNTS.getOrDefault(orderId, new AtomicInteger(0)).get();
    }

    public static void reset(String orderId) {
        COUNTS.remove(orderId);
    }
}
