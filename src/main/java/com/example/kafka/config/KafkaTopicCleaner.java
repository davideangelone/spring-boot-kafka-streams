package com.example.kafka.config;

import java.util.List;
import java.util.Set;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Cancella i topic dell'applicazione all'avvio: utile solo in locale/POC, distruttivo altrove.
 * Attivo solo con {@code app.topics.clean-on-startup=true}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "app.topics", name = "clean-on-startup", havingValue = "true")
public class KafkaTopicCleaner {

    private final KafkaAdmin kafkaAdmin;
    private final AppKafkaProperties properties;
    private final String applicationId;

    public KafkaTopicCleaner(
            KafkaAdmin kafkaAdmin,
            AppKafkaProperties properties,
            @Value("${spring.kafka.streams.application-id}") String applicationId) {
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
        this.applicationId = applicationId;
    }

    @PostConstruct
    public void cleanTopics() {

        String changelogTopicName = String.format("%s-%s-changelog", applicationId, properties.getStateStore());

        List<String> topics = List.of(
                properties.getTopics().getOrders(),
                properties.getTopics().getNotifications(),
                properties.getRetryTopics().getOrders(),
                properties.getDlqTopics().getOrders(),
                changelogTopicName
        );

        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {

            Set<String> existingTopics = admin.listTopics().names().get();

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
