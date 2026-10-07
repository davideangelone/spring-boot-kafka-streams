package com.example.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import com.example.kafka.stream.OrderProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class OrderRetryListenerTest {

    private static final String NOTIFICATIONS = "notifications-topic";

    private KafkaTemplate<String, Object> template;
    private OrderRetryListener listener;
    private OrderRetry retry;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        template = mock(KafkaTemplate.class);
        AppKafkaProperties props = new AppKafkaProperties();
        props.getTopics().setNotifications(NOTIFICATIONS);

        listener = new OrderRetryListener(mock(OrderProcessor.class), template, props);

        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, 1L);
        retry = new OrderRetry("e-1", order, "orders-topic", 0, 10L, 1L, "java.lang.RuntimeException", "x");
    }

    @Test
    void publishesTheOrderOnNotifications() {
        when(template.send(eq(NOTIFICATIONS), eq("o-1"), any())).thenReturn(CompletableFuture.completedFuture(null));

        listener.process(retry);

        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(template).send(eq(NOTIFICATIONS), eq("o-1"), payload.capture());
        // stesso schema di quello scritto dalla topologia Streams
        assertThat(payload.getValue()).isEqualTo(retry.order());
    }

    @Test
    void sendFailurePropagatesSoRetryAndDlqCanKickIn() {
        when(template.send(eq(NOTIFICATIONS), eq("o-1"), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> listener.process(retry)).isInstanceOf(CompletionException.class);
    }
}
