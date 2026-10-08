package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamingApplication.class, args);
    }
}
