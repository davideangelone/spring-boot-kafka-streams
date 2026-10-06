package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.resilience.annotation.EnableResilientMethods;

@SpringBootApplication
@EnableKafkaStreams
@EnableResilientMethods
public class KafkaStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamingApplication.class, args);
    }
}
