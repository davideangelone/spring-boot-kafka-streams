package com.example.kafka.dlq;

import static com.example.kafka.constants.Headers.HEADER_EXCEPTION_CLASS;
import static com.example.kafka.constants.Headers.WORKER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import com.example.kafka.config.AppKafkaProperties;
import com.example.kafka.model.Order;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class DlqPublisherTest {

    private KafkaTemplate<String, Object> template;
    private DlqPublisher publisher;

    @BeforeEach
    @SuppressWarnings({"unchecked"})
    void setUp() {
        template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        AppKafkaProperties props = new AppKafkaProperties();
        props.getDlqTopics().setOrders("orders-dlq");
        publisher = new DlqPublisher(template, props, new DlqService());
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, Object> sentRecord() {
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    @Test
    void oversizedOrderIsSentCompactedAndWithDiagnosticHeaders() {
        String huge = "X".repeat(10_000);
        Order order = new Order("o-1", "CUST-1", huge, 1, 1234L);

        publisher.publish(order, "3", new RuntimeException(huge));

        ProducerRecord<String, Object> sent = sentRecord();
        assertThat(sent.topic()).isEqualTo("orders-dlq");
        assertThat(sent.key()).isEqualTo("o-1");
        assertThat(sent.timestamp()).isEqualTo(1234L);
        assertThat(sent.value()).isInstanceOfSatisfying(Order.class, payload -> {
            assertThat(payload.orderId()).isEqualTo("o-1");
            assertThat(payload.productId()).hasSize(DlqService.MAX_FIELD_LENGTH);
        });
        assertThat(new String(sent.headers().lastHeader(WORKER_ID).value(), StandardCharsets.UTF_8)).isEqualTo("3");
        assertThat(sent.headers().lastHeader(HEADER_EXCEPTION_CLASS)).isNotNull();

        // Header e chiave restano ampiamente sotto max.request.size (4096): la DLQ non deve scartare a sua volta
        int headersSize = 0;
        for (Header header : sent.headers()) {
            headersSize += header.key().length() + header.value().length;
        }
        assertThat(headersSize + sent.key().length()).isLessThan(1024);
    }

    @Test
    void negativeOrderTimestampDoesNotMakeTheSendFail() {
        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, -5L);

        publisher.publish(order, "1", null);

        assertThat(sentRecord().timestamp()).isZero();
    }
}
