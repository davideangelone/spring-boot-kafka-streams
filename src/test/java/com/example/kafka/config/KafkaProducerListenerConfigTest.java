package com.example.kafka.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import com.example.kafka.dlq.DlqPublisher;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

class KafkaProducerListenerConfigTest {

    private static final String ORDERS = "orders-topic";

    private DlqPublisher dlqPublisher;
    private ProducerListener<String, Object> listener;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        dlqPublisher = mock(DlqPublisher.class);

        AppKafkaProperties props = new AppKafkaProperties();
        props.getTopics().setOrders(ORDERS);
        props.getTopics().setNotifications("notifications-topic");
        props.getDlqTopics().setOrders("orders-dlq");

        new KafkaProducerListenerConfig(template, dlqPublisher, props).registerListener();

        ArgumentCaptor<ProducerListener<String, Object>> captor = ArgumentCaptor.forClass(ProducerListener.class);
        verify(template).setProducerListener(captor.capture());
        listener = captor.getValue();
    }

    private static Order order() {
        return new Order("o-1", "CUST-1", "PROD-1", 1, 1L);
    }

    @Test
    void failureWithoutExplicitPartitionStillReachesDlq() {
        // OrderProducer non imposta la partizione: partition() è null (regressione NPE)
        ProducerRecord<String, Object> record = new ProducerRecord<>(ORDERS, "PROD-1", order());
        record.headers().add("workerId", "3".getBytes(StandardCharsets.UTF_8));

        listener.onError(record, null, new RuntimeException("boom"));

        ArgumentCaptor<OrderRetry> retry = ArgumentCaptor.forClass(OrderRetry.class);
        ArgumentCaptor<String> source = ArgumentCaptor.forClass(String.class);
        verify(dlqPublisher).publish(org.mockito.ArgumentMatchers.eq("PROD-1"), retry.capture(), source.capture(), any());

        assertThat(retry.getValue().originalPartition()).isEqualTo(-1);
        assertThat(retry.getValue().originalTopic()).isEqualTo(ORDERS);
        assertThat(retry.getValue().errorType()).isEqualTo(RuntimeException.class.getName());
        assertThat(retry.getValue().errorMessage()).isEqualTo("boom");
        assertThat(retry.getValue().order()).isEqualTo(order());
        assertThat(source.getValue()).contains("worker=3");
    }

    @Test
    void explicitPartitionIsPreserved() {
        ProducerRecord<String, Object> record = new ProducerRecord<>(ORDERS, 2, "PROD-1", order());

        listener.onError(record, null, new RuntimeException("boom"));

        ArgumentCaptor<OrderRetry> retry = ArgumentCaptor.forClass(OrderRetry.class);
        verify(dlqPublisher).publish(any(), retry.capture(), any(), any());
        assertThat(retry.getValue().originalPartition()).isEqualTo(2);
    }

    @Test
    void failureOnOtherTopicsIsNotRedirectedToDlq() {
        listener.onError(new ProducerRecord<>("notifications-topic", "o-1", order()), null, new RuntimeException("boom"));
        // un fallimento sulla DLQ stessa non deve generare un nuovo invio in DLQ (niente ricorsione)
        listener.onError(new ProducerRecord<>("orders-dlq", "o-1", "qualsiasi"), null, new RuntimeException("boom"));

        verify(dlqPublisher, never()).publish(any(), any(), any(), any());
    }
}
