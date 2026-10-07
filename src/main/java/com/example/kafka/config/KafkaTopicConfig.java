package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    private final AppKafkaProperties appProperties;

    public KafkaTopicConfig(AppKafkaProperties appProperties) {
        this.appProperties = appProperties;
    }

    // Main topics
    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic(appProperties.getTopics().getOrders(), appProperties.getTopics().getPartitions(), (short) 1);
    }

    @Bean
    public NewTopic notificationsTopic() {
        return new NewTopic(appProperties.getTopics().getNotifications(), appProperties.getTopics().getPartitions(), (short) 1);
    }

    // Retry topics
    @Bean
    public NewTopic ordersRetryTopic() {
        return new NewTopic(appProperties.getRetryTopics().getOrders(), appProperties.getTopics().getPartitions(), (short) 1);
    }

    // DLQ topics
    @Bean
    public NewTopic ordersDlqTopic() {
        return new NewTopic(appProperties.getDlqTopics().getOrders(), appProperties.getTopics().getPartitions(), (short) 1);
    }
}
