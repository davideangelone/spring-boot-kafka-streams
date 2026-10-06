package com.example.kafka.stream;

import java.util.Map;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class OrderStreamTopology {

    private final OrderProcessor orderProcessor;

    public OrderStreamTopology(OrderProcessor orderProcessor) {
        this.orderProcessor = orderProcessor;
    }

    @Bean
    public KStream<String, OrderProcessingResult> orderStream(
            AppKafkaProperties appProperties,
            StreamsBuilder builder,
            Serde<Order> orderSerde,
            Serde<OrderRetry> orderRetrySerde) {

        KStream<String, Order> orders =
                builder.stream(
                        appProperties.getTopics().getOrders(),
                        Consumed.with(
                                Serdes.String(),
                                orderSerde
                        )
                );

        KStream<String, OrderProcessingResult> results =
                orders.process(
                        () -> new OrderProcessingProcessor(orderProcessor),
                        Named.as("order-processing")
                );

        Map<String, KStream<String, OrderProcessingResult>> branches =
                results.split(Named.as("processing-"))
                        .branch(
                                (key, result) -> result.isSuccess(),
                                Branched.as("success")
                        )
                        .branch(
                                (key, result) -> !result.isSuccess(),
                                Branched.as("failure")
                        )
                        .noDefaultBranch();

        KStream<String, OrderProcessingResult> successfulOrders = branches.get("processing-success");
        KStream<String, OrderProcessingResult> failedOrders = branches.get("processing-failure");

        // OK
        successfulOrders
                .mapValues(OrderProcessingResult::order)
                .to(
                        appProperties.getTopics().getNotifications(),
                        Produced.with(
                                Serdes.String(),
                                orderSerde
                        )
                );

        // KO -> retry
        failedOrders
                .mapValues(OrderProcessingResult::retry)
                .to(
                        appProperties.getRetryTopics().getOrders(),
                        Produced.with(
                                Serdes.String(),
                                orderRetrySerde
                        )
                );

        return results;
    }

}
