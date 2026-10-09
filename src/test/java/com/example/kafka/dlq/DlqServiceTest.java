package com.example.kafka.dlq;

import static com.example.kafka.constants.Headers.HEADER_EXCEPTION_CLASS;
import static com.example.kafka.constants.Headers.HEADER_EXCEPTION_MESSAGE;
import static com.example.kafka.constants.Headers.UNKNOWN_WORKER_ID;
import static com.example.kafka.constants.Headers.WORKER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import com.example.kafka.model.Order;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;

class DlqServiceTest {

    private final DlqService service = new DlqService();

    private static String header(Record<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    void recordUsesOrderIdAsKeyAndCompactsTheOrder() {
        String huge = "X".repeat(10_000);
        Order order = new Order("o-1", huge, huge, 3, 1234L);

        Record<String, Order> record = service.createDlqRecord(order, "3", new RuntimeException("boom"));

        assertThat(record.key()).isEqualTo("o-1");
        assertThat(record.timestamp()).isEqualTo(1234L);
        assertThat(record.value().orderId()).isEqualTo("o-1");
        assertThat(record.value().customerId()).hasSize(DlqService.MAX_FIELD_LENGTH);
        assertThat(record.value().productId()).hasSize(DlqService.MAX_FIELD_LENGTH);
        assertThat(record.value().quantity()).isEqualTo(3);
        assertThat(header(record, WORKER_ID)).isEqualTo("3");
        assertThat(header(record, HEADER_EXCEPTION_CLASS)).isEqualTo(RuntimeException.class.getName());
        assertThat(header(record, HEADER_EXCEPTION_MESSAGE)).isEqualTo("boom");
    }

    @Test
    void longExceptionMessageIsTruncated() {
        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, 1L);

        Record<String, Order> record = service.createDlqRecord(order, "1", new RuntimeException("x".repeat(5_000)));

        assertThat(header(record, HEADER_EXCEPTION_MESSAGE)).hasSize(DlqService.MAX_MESSAGE_LENGTH);
    }

    @Test
    void exceptionWithoutMessageDoesNotFail() {
        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, 1L);

        Record<String, Order> record = service.createDlqRecord(order, "1", new NullPointerException());

        assertThat(header(record, HEADER_EXCEPTION_CLASS)).isEqualTo(NullPointerException.class.getName());
        assertThat(header(record, HEADER_EXCEPTION_MESSAGE)).isEmpty();
    }

    @Test
    void withoutCauseThereAreNoExceptionHeaders() {
        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, 1L);

        Record<String, Order> record = service.createDlqRecord(order, "1", null);

        assertThat(record.headers().lastHeader(HEADER_EXCEPTION_CLASS)).isNull();
        assertThat(record.headers().lastHeader(HEADER_EXCEPTION_MESSAGE)).isNull();
        assertThat(header(record, WORKER_ID)).isEqualTo("1");
    }

    @Test
    void missingWorkerIdBecomesUnknown() {
        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, 1L);

        Record<String, Order> record = service.createDlqRecord(order, null, null);

        assertThat(header(record, WORKER_ID)).isEqualTo(UNKNOWN_WORKER_ID);
    }

    @Test
    void negativeTimestampIsClampedOnTheRecordButKeptInThePayload() {
        Order order = new Order("o-1", "CUST-1", "PROD-1", 1, -5L);

        Record<String, Order> record = service.createDlqRecord(order, "1", null);

        assertThat(record.timestamp()).isZero();
        assertThat(record.value().timestamp()).isEqualTo(-5L);
    }

    @Test
    void nullOrderIsRejected() {
        assertThatThrownBy(() -> service.createDlqRecord(null, "1", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void workerIdIsReadFromHeadersAndDefaultsToUnknown() {
        RecordHeaders withWorker = new RecordHeaders();
        withWorker.add(new RecordHeader(WORKER_ID, "5".getBytes(StandardCharsets.UTF_8)));

        assertThat(service.getWorkerId(withWorker)).isEqualTo("5");
        assertThat(service.getWorkerId(new RecordHeaders())).isEqualTo(UNKNOWN_WORKER_ID);
    }
}
