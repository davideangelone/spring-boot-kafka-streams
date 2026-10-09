package com.example.kafka.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import com.example.kafka.dlq.DlqPublisher;
import com.example.kafka.dlq.DlqService;
import com.example.kafka.model.Order;
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

        new KafkaProducerListenerConfig(template, dlqPublisher, new DlqService(), props).registerListener();

        ArgumentCaptor<ProducerListener<String, Object>> captor = ArgumentCaptor.forClass(ProducerListener.class);
        verify(template).setProducerListener(captor.capture());
        listener = captor.getValue();
    }

    private static Order order() {
        return new Order("o-1", "CUST-1", "PROD-1", 1, 1L);
    }

    @Test
    void failureOnOrdersTopicIsRedirectedToDlqWithWorkerIdAndCause() {
        ProducerRecord<String, Object> record = new ProducerRecord<>(ORDERS, "o-1", order());
        record.headers().add("workerId", "3".getBytes(StandardCharsets.UTF_8));
        RuntimeException failure = new RuntimeException("boom");

        listener.onError(record, null, failure);

        verify(dlqPublisher).publish(eq(order()), eq("3"), same(failure));
    }

    @Test
    void missingWorkerIdHeaderMeansUnknownWorker() {
        ProducerRecord<String, Object> record = new ProducerRecord<>(ORDERS, "o-1", order());
        RuntimeException failure = new RuntimeException("boom");

        listener.onError(record, null, failure);

        verify(dlqPublisher).publish(eq(order()), eq(DlqService.UNKNOWN_WORKER_ID), same(failure));
    }

    @Test
    void failuresOnOtherTopicsOrWithOtherPayloadsAreNotRedirected() {
        RuntimeException failure = new RuntimeException("boom");

        listener.onError(new ProducerRecord<>("notifications-topic", "o-1", order()), null, failure);
        // un fallimento sulla DLQ stessa non deve generare un nuovo invio in DLQ (niente ricorsione)
        listener.onError(new ProducerRecord<>("orders-dlq", "o-1", order()), null, failure);
        // payload che non è un Order
        listener.onError(new ProducerRecord<>(ORDERS, "k", "non-un-ordine"), null, failure);

        verify(dlqPublisher, never()).publish(any(), any(), any());
    }
}
