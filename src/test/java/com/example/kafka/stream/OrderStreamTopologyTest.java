package com.example.kafka.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.dlq.DlqService;
import com.example.kafka.model.Order;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

class OrderStreamTopologyTest {

    private static final String ORDERS = "orders-topic";
    private static final String NOTIFICATIONS = "notifications-topic";
    private static final String RETRY = "orders-retry";
    private static final String DLQ = "orders-dlq";
    private static final String RETRY_STORE = "order-retry-counts";

    private final Serde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
    private final Serde<String> stringSerde = Serdes.String();

    private TopologyTestDriver driver;
    private TestInputTopic<String, Order> input;
    private TestOutputTopic<String, Order> notifications;
    private TestOutputTopic<String, Order> retries;
    private TestOutputTopic<String, Order> dlq;

    private void startTopologyFails() {
        startTopology(1.0, 1, 1);
    }

    private void startTopologySucceeds() {
        startTopology(0.0, 1, -1);
    }

    private void startTopology(double errorRate, int errorTimestampModulo, int errorTimestampThreshold) {
        startTopology(new OrderProcessor(errorRate, errorTimestampModulo, errorTimestampThreshold));
    }

    private void startTopology(OrderProcessor orderProcessor) {
        AppKafkaProperties props = new AppKafkaProperties();
        props.getTopics().setOrders(ORDERS);
        props.getTopics().setNotifications(NOTIFICATIONS);
        props.getRetryTopics().setOrders(RETRY);
        props.getDlqTopics().setOrders(DLQ);
        props.setStateStore(RETRY_STORE);

        StreamsBuilder builder = new StreamsBuilder();
        DlqService dlqService = new DlqService();
        new OrderStreamTopology(props, orderProcessor, dlqService).orderStream(builder, new Serdes.StringSerde(), orderSerde);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        input = driver.createInputTopic(ORDERS, stringSerde.serializer(), orderSerde.serializer());
        notifications = driver.createOutputTopic(NOTIFICATIONS, stringSerde.deserializer(), orderSerde.deserializer());
        retries = driver.createOutputTopic(RETRY, stringSerde.deserializer(), orderSerde.deserializer());
        dlq = driver.createOutputTopic(DLQ, stringSerde.deserializer(), orderSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        stringSerde.close();
        orderSerde.close();
    }

    private KeyValueStore<String, Integer> retryStore() {
        return driver.getKeyValueStore(RETRY_STORE);
    }

    private static String header(TestRecord<String, Order> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void successfulOrderGoesToNotifications() {
        startTopologySucceeds();
        Order order = new Order("o-1", "CUST-1", "PROD-1", 2, 500L);

        input.pipeInput("o-1", order);

        assertThat(notifications.readKeyValuesToList()).hasSize(1)
                .first().satisfies(kv -> {
                    assertThat(kv.key).isEqualTo("o-1");
                    assertThat(kv.value).isEqualTo(order);
                });
        assertThat(retries.isEmpty()).isTrue();
        assertThat(dlq.isEmpty()).isTrue();
        assertThat(retryStore().get("o-1")).isNull();
    }

    @Test
    void failedOrderGoesToRetryWithFailureMetadata() {
        startTopologyFails();
        Order order = new Order("o-2", "CUST-2", "PROD-2", 1, 2L);

        input.pipeInput("PROD-2", order);

        assertThat(notifications.isEmpty()).isTrue();
        assertThat(retries.readKeyValuesToList()).hasSize(2)
                .first().satisfies(kv -> {
                    Order retry = kv.value;
                    assertThat(kv.key).isEqualTo("PROD-2");
                    assertThat(retry).isEqualTo(order);
                });
    }

    @Test
    void orderFailingEveryTimeIsRetriedThreeTimesThenSentToDlqWithDiagnosticHeaders() {
        startTopologyFails();
        Order order = new Order("o-2", "CUST-2", "PROD-2", 1, 2L);
        Headers headers = new RecordHeaders().add(new RecordHeader("workerId", "7".getBytes(StandardCharsets.UTF_8)));

        input.pipeInput(new TestRecord<>("o-2", order, headers));

        assertThat(notifications.isEmpty()).isTrue();
        assertThat(retries.readValuesToList()).hasSize(2);

        List<TestRecord<String, Order>> dead = dlq.readRecordsToList();
        assertThat(dead).hasSize(1);
        TestRecord<String, Order> record = dead.getFirst();
        assertThat(record.key()).isEqualTo("o-2");
        assertThat(record.value()).isEqualTo(order);
        assertThat(header(record, DlqService.HEADER_EXCEPTION_CLASS)).isEqualTo(RuntimeException.class.getName());
        assertThat(header(record, DlqService.HEADER_EXCEPTION_MESSAGE)).contains("Simulated processing error");
        assertThat(header(record, DlqService.WORKER_ID)).isEqualTo("7");

        // Il contatore dei tentativi non deve restare nello state store dopo l'invio in DLQ
        assertThat(retryStore().get("o-2")).isNull();
    }

    @Test
    void orderFailingOnceThenSucceedingClearsTheRetryState() {
        OrderProcessor processor = mock(OrderProcessor.class);
        doThrow(new RuntimeException("transient")).doNothing().when(processor).process(any());
        startTopology(processor);
        Order order = new Order("o-3", "CUST-3", "PROD-3", 1, 500L);

        input.pipeInput("o-3", order);

        assertThat(retries.readValuesToList()).containsExactly(order);
        assertThat(notifications.readValuesToList()).containsExactly(order);
        assertThat(dlq.isEmpty()).isTrue();
        assertThat(retryStore().get("o-3")).isNull();
    }

    @Test
    void recordWithNullValueIsIgnored() {
        startTopology(0.0, 1, -1);

        input.pipeInput("k-0", null);

        assertThat(notifications.isEmpty()).isTrue();
        assertThat(retries.isEmpty()).isTrue();
        assertThat(dlq.isEmpty()).isTrue();
    }

    @Test
    void orderWithoutOrderIdGoesStraightToDlq() {
        startTopology(0.0, 1, -1);
        Order order = new Order(null, "CUST-4", "PROD-4", 1, 500L);

        input.pipeInput("k-4", order);

        assertThat(notifications.isEmpty()).isTrue();
        assertThat(retries.isEmpty()).isTrue();
        List<TestRecord<String, Order>> dead = dlq.readRecordsToList();
        assertThat(dead).hasSize(1);
        assertThat(dead.getFirst().value()).isEqualTo(order);
        assertThat(header(dead.getFirst(), DlqService.HEADER_EXCEPTION_CLASS)).isEqualTo(IllegalArgumentException.class.getName());
        assertThat(header(dead.getFirst(), DlqService.HEADER_EXCEPTION_MESSAGE)).isEqualTo("Missing orderId");
    }
}
