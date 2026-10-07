package com.example.kafka.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

class OrderStreamTopologyTest {

    private static final String ORDERS = "orders-topic";
    private static final String NOTIFICATIONS = "notifications-topic";
    private static final String RETRY = "orders-retry";

    private final Serde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
    private final Serde<OrderRetry> retrySerde = new JacksonJsonSerde<>(OrderRetry.class);
    private final Serde<String> stringSerde = Serdes.String();

    private TopologyTestDriver driver;
    private TestInputTopic<String, Order> input;
    private TestOutputTopic<String, Order> notifications;
    private TestOutputTopic<String, OrderRetry> retries;

    private void startTopology(double errorRate) {
        AppKafkaProperties props = new AppKafkaProperties();
        props.getTopics().setOrders(ORDERS);
        props.getTopics().setNotifications(NOTIFICATIONS);
        props.getRetryTopics().setOrders(RETRY);
        props.getDlqTopics().setOrders("orders-dlq");

        StreamsBuilder builder = new StreamsBuilder();
        new OrderStreamTopology(new OrderProcessor(errorRate, 0.0)).orderStream(props, builder, orderSerde, retrySerde);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        input = driver.createInputTopic(ORDERS, stringSerde.serializer(), orderSerde.serializer());
        notifications = driver.createOutputTopic(NOTIFICATIONS, stringSerde.deserializer(), orderSerde.deserializer());
        retries = driver.createOutputTopic(RETRY, stringSerde.deserializer(), retrySerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        stringSerde.close();
        orderSerde.close();
        retrySerde.close();
    }

    @Test
    void successfulOrderGoesToNotifications() {
        startTopology(0.0);
        Order order = new Order("o-1", "CUST-1", "PROD-1", 2, 1L);

        input.pipeInput("PROD-1", order);

        assertThat(notifications.readKeyValuesToList()).hasSize(1)
                .first().satisfies(kv -> {
                    assertThat(kv.key).isEqualTo("PROD-1");
                    assertThat(kv.value).isEqualTo(order);
                });
        assertThat(retries.isEmpty()).isTrue();
    }

    @Test
    void failedOrderGoesToRetryWithFailureMetadata() {
        startTopology(1.0); // nextDouble() < 1.0: fallisce sempre
        Order order = new Order("o-2", "CUST-2", "PROD-2", 1, 2L);

        input.pipeInput("PROD-2", order);

        assertThat(notifications.isEmpty()).isTrue();
        assertThat(retries.readKeyValuesToList()).hasSize(1)
                .first().satisfies(kv -> {
                    OrderRetry retry = kv.value;
                    assertThat(kv.key).isEqualTo("PROD-2");
                    assertThat(retry.order()).isEqualTo(order);
                    assertThat(retry.originalTopic()).isEqualTo(ORDERS);
                    assertThat(retry.errorType()).isEqualTo(RuntimeException.class.getName());
                    assertThat(retry.errorMessage()).contains("Simulated processing error");
                });
    }
}
