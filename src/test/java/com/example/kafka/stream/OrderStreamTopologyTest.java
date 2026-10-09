package com.example.kafka.stream;

import java.util.Properties;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.dlq.DlqService;
import com.example.kafka.model.Order;
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

import static org.assertj.core.api.Assertions.assertThat;

class OrderStreamTopologyTest {

    private static final String ORDERS = "orders-topic";
    private static final String NOTIFICATIONS = "notifications-topic";
    private static final String RETRY = "orders-retry";

    private final Serde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
    private final Serde<String> stringSerde = Serdes.String();

    private TopologyTestDriver driver;
    private TestInputTopic<String, Order> input;
    private TestOutputTopic<String, Order> notifications;
    private TestOutputTopic<String, Order> retries;

    private void startTopology(double errorRate) {
        AppKafkaProperties props = new AppKafkaProperties();
        props.getTopics().setOrders(ORDERS);
        props.getTopics().setNotifications(NOTIFICATIONS);
        props.getRetryTopics().setOrders(RETRY);
        props.getDlqTopics().setOrders("orders-dlq");

        StreamsBuilder builder = new StreamsBuilder();
        OrderProcessor orderProcessor = new OrderProcessor(errorRate);
        DlqService dlqService = new DlqService();
        new OrderStreamTopology(props, orderProcessor, dlqService).orderStream(builder, new Serdes.StringSerde(), orderSerde);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        input = driver.createInputTopic(ORDERS, stringSerde.serializer(), orderSerde.serializer());
        notifications = driver.createOutputTopic(NOTIFICATIONS, stringSerde.deserializer(), orderSerde.deserializer());
        retries = driver.createOutputTopic(RETRY, stringSerde.deserializer(), orderSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        stringSerde.close();
        orderSerde.close();
    }

    @Test
    void failedOrderGoesToRetryWithFailureMetadata() {
        startTopology(1.0); // nextDouble() < 1.0: fallisce sempre
        Order order = new Order("o-2", "CUST-2", "PROD-2", 1, 2L);

        input.pipeInput("PROD-2", order);

        assertThat(notifications.isEmpty()).isTrue();
        assertThat(retries.readKeyValuesToList()).hasSize(3)
                .first().satisfies(kv -> {
                    Order retry = kv.value;
                    assertThat(kv.key).isEqualTo("PROD-2");
                    assertThat(retry).isEqualTo(order);
                });
    }
}
