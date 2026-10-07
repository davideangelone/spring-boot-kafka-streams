package com.example.kafka.inspector;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class KafkaTopicInspector {

    public static void main(String[] args) {

        KafkaTopicInspector inspector =
                new KafkaTopicInspector("localhost:9092");

        var result =
                inspector.inspect("notifications-topic");

        result.printReport();
    }

    private final String bootstrapServers;

    public KafkaTopicInspector(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Analizza un topic e conta i record applicativi effettivamente
     * leggibili con isolation.level=read_committed.
     */
    public InspectionResult inspect(String topic) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        /*
         * Il group id è casuale perché l'inspector deve essere indipendente
         * dai consumer group dell'applicazione.
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-poc-inspector-" + UUID.randomUUID());

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        /*
         * Fondamentale:
         *
         * READ_COMMITTED:
         * - legge record non transazionali
         * - legge record di transazioni COMMITTATE
         * - ignora record appartenenti a transazioni ABORTED
         * - non espone i transaction/control records come messaggi applicativi
         */
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
         * Disabilitiamo il commit automatico.
         * L'inspector non deve modificare lo stato di nessun consumer group.
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Map<Integer, PartitionStats> partitionStats = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            List<PartitionInfo> partitions =
                    consumer.partitionsFor(topic)
                            .stream()
                            .map(p -> new PartitionInfo(
                                    p.partition()
                            ))
                            .toList();

            List<TopicPartition> topicPartitions =
                    partitions.stream()
                            .map(p -> new TopicPartition(topic, p.partition()))
                            .toList();

            consumer.assign(topicPartitions);

            /*
             * Partiamo dall'inizio del topic.
             */
            consumer.seekToBeginning(topicPartitions);

            /*
             * Determiniamo gli offset iniziali e finali.
             */
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            for (TopicPartition partition : topicPartitions) {

                long beginning = beginningOffsets.getOrDefault(partition, 0L);
                long end = endOffsets.getOrDefault(partition, 0L);

                partitionStats.put(
                        partition.partition(),
                        new PartitionStats(partition.partition(), beginning, end)
                );
            }

            /*
             * Leggiamo tutti i record visibili con READ_COMMITTED.
             *
             * Polliamo fino a quando non abbiamo raggiunto
             * gli end offsets iniziali.
             */
            while (hasRemainingRecords(consumer, endOffsets)) {

                var records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    PartitionStats stats = partitionStats.get(record.partition());

                    if (stats != null) {
                        stats.incrementEffectiveRecords();
                    }

                    log.debug(
                            "Partition={} Offset={} Key={}",
                            record.partition(),
                            record.offset(),
                            record.key()
                    );
                }
            }

            return new InspectionResult(topic, partitionStats);
        }
    }

    private boolean hasRemainingRecords(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> endOffsets) {

        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {

            TopicPartition partition = entry.getKey();
            long endOffset = entry.getValue();
            long currentPosition = consumer.position(partition);

            if (currentPosition < endOffset) {
                return true;
            }
        }

        return false;
    }

    public record PartitionInfo(int partition) {
    }

    @Getter
    public static class PartitionStats {
        private final int partition;
        private final long firstOffset;
        private final long nextOffset;
        private long effectiveRecords;

        public PartitionStats(int partition, long firstOffset, long nextOffset) {
            this.partition = partition;
            this.firstOffset = firstOffset;
            this.nextOffset = nextOffset;
        }

        public void incrementEffectiveRecords() {
            effectiveRecords++;
        }

        public long getKafkaOffsetCount() {
            return nextOffset - firstOffset;
        }

    }

    public record InspectionResult(String topic, Map<Integer, PartitionStats> partitions) {

        public long getKafkaOffsetCount() {
                return partitions.values()
                        .stream()
                        .mapToLong(PartitionStats::getKafkaOffsetCount)
                        .sum();
            }

            public long getEffectiveRecordCount() {
                return partitions.values()
                        .stream()
                        .mapToLong(PartitionStats::getEffectiveRecords)
                        .sum();
            }

            public void printReport() {

                log.info("");
                log.info("==============================================");
                log.info(" Kafka Topic Inspector");
                log.info("==============================================");
                log.info("Topic: {}", topic);
                log.info("");
                log.info("{}",
                        String.format(
                                "%-10s %-15s %-15s %-20s",
                                "Partition",
                                "First Offset",
                                "Next Offset",
                                "Effective Records"
                        )
                );

                log.info("--------------------------------------------------------------");

                partitions.values()
                        .stream()
                        .sorted(Comparator.comparingInt(PartitionStats::getPartition))
                        .forEach(stats ->
                                log.info("{}",
                                        String.format(
                                                "%-10d %-15d %-15d %-20d",
                                                stats.getPartition(),
                                                stats.getFirstOffset(),
                                                stats.getNextOffset(),
                                                stats.getEffectiveRecords()
                                        )
                                )
                        );

                log.info("--------------------------------------------------------------");
                log.info("Kafka offset positions : {}", getKafkaOffsetCount());
                log.info("Effective records      : {}", getEffectiveRecordCount());
                log.info("==============================================");
            }
        }
}

