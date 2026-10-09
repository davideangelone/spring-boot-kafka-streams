package com.example.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties(prefix = "app")
public class AppKafkaProperties {
    private Topics topics = new Topics();
    private RetryTopics retryTopics = new RetryTopics();
    private DlqTopics dlqTopics = new DlqTopics();
    private String stateStore;

    @Data
    public static class Topics {
        private String orders;
        private String notifications;
        private int partitions = 5;
        private boolean cleanOnStartup = false; // Cancella i topic all'avvio
    }

    @Data
    public static class RetryTopics {
        private String orders;
    }

    @Data
    public static class DlqTopics {
        private String orders;
    }
}
