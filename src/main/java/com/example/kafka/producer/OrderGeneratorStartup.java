package com.example.kafka.producer;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class OrderGeneratorStartup {

    private final OrderEventGenerator generator;

    public OrderGeneratorStartup(OrderEventGenerator generator) {
        this.generator = generator;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        generator.start();
    }
}
