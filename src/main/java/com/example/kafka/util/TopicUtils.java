package com.example.kafka.util;

public class TopicUtils {

    /**
     * Map a topic to its DLQ (Dead Letter Queue) equivalent.
     *
     * @param topic the original topic name
     * @return the DLQ topic name
     */
    public static String mapTopicToDlq(String topic) {
        return topic.replace("topic", "dlq");
    }

    /**
     * Checks if the topic is a DLQ (Dead Letter Queue).
     *
     * @param topic the original topic name
     * @return true if the topic is a DLQ, false otherwise
     */
    public static boolean isDlqTopic(String topic) {
        return topic.endsWith("dlq");
    }
}
