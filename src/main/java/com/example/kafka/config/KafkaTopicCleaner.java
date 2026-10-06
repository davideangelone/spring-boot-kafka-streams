package com.example.kafka.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Slf4j
@Component
public class KafkaTopicCleaner {

    private final KafkaAdmin kafkaAdmin;
    private final AppKafkaProperties properties;

    public KafkaTopicCleaner(
            KafkaAdmin kafkaAdmin,
            AppKafkaProperties properties) {
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
    }

    @PostConstruct
    public void cleanTopics() {

        List<String> topics = List.of(
                properties.getTopics().getOrders(),
                properties.getTopics().getNotifications(),
                properties.getRetryTopics().getOrders(),
                properties.getDlqTopics().getOrders()
        );

        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {

            Set<String> existingTopics =admin.listTopics().names().get();

            List<String> topicsToDelete = topics.stream()
                    .filter(existingTopics::contains)
                    .toList();

            if (!topicsToDelete.isEmpty()) {
                log.info("Deleting Kafka topics: {}", topicsToDelete);

                admin.deleteTopics(topicsToDelete)
                        .all()
                        .get();
            }

        } catch (Exception e) {
            throw new IllegalStateException("Unable to clean Kafka topics", e);
        }
    }
}
