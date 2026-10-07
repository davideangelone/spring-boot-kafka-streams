package com.example.kafka.dlq;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderRetry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqPublisherTest {

    @Test
    @SuppressWarnings("unchecked")
    void oversizedMessageIsCompactedBeforeBeingSentToDlq() {
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        AppKafkaProperties props = new AppKafkaProperties();
        props.getDlqTopics().setOrders("orders-dlq");
        DlqPublisher publisher = new DlqPublisher(template, props);

        String huge = "X".repeat(10_000);
        Order order = new Order("o-1", "CUST-1", huge, 1, 1L);
        OrderRetry retry = new OrderRetry("e-1", order, "orders-topic", -1, -1L, 1L,
                "org.apache.kafka.common.errors.RecordTooLargeException", huge);

        publisher.publish(huge, retry, "producer[worker=1]", new IllegalStateException(huge));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        ProducerRecord<String, Object> sent = captor.getValue();

        assertThat(sent.topic()).isEqualTo("orders-dlq");
        assertThat(sent.key()).hasSizeLessThanOrEqualTo(DlqPublisher.MAX_FIELD_LENGTH);

        OrderRetry payload = (OrderRetry) sent.value();
        assertThat(payload.order().productId()).hasSizeLessThanOrEqualTo(DlqPublisher.MAX_FIELD_LENGTH);
        assertThat(payload.errorMessage()).hasSizeLessThanOrEqualTo(DlqPublisher.MAX_MESSAGE_LENGTH);
        assertThat(payload.order().orderId()).isEqualTo("o-1");

        assertThat(new String(sent.headers().lastHeader(DlqPublisher.HEADER_SOURCE).value(), StandardCharsets.UTF_8))
                .isEqualTo("producer[worker=1]");
        assertThat(sent.headers().lastHeader(DlqPublisher.HEADER_EXCEPTION_CLASS)).isNotNull();
    }

    @Test
    void truncateKeepsShortValuesAndHandlesNull() {
        assertThat(DlqPublisher.truncate(null, 10)).isNull();
        assertThat(DlqPublisher.truncate("abc", 10)).isEqualTo("abc");
        assertThat(DlqPublisher.truncate("abcdefghijkl", 10)).hasSize(10).endsWith("...");
    }
}
