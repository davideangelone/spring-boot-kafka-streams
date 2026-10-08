package com.example.kafka.config;

import com.example.kafka.model.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
public class KafkaSerdeConfig {

    @Bean
    public Serdes.StringSerde stringSerde() {
        return new Serdes.StringSerde();
    }

    @Bean
    public Serde<Order> orderSerde() {
        return new JacksonJsonSerde<>(Order.class);
    }

}
