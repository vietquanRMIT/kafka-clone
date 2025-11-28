package com.example.kafkaclone.service;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks committed offsets per consumer group in-memory.
 */
@Component
public class OffsetManager {

    // consumerGroup -> topic -> partition -> offset
    private final Map<String, Map<String, Map<Integer, Long>>> committedOffsets = new ConcurrentHashMap<>();

    public void commitOffset(String consumerGroupId, String topic, int partition, long offset) {
        committedOffsets
                .computeIfAbsent(consumerGroupId, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
                .put(partition, offset);
    }

    public Optional<Long> readOffset(String consumerGroupId, String topic, int partition) {
        return Optional.ofNullable(
                committedOffsets.getOrDefault(consumerGroupId, Map.of())
                        .getOrDefault(topic, Map.of())
                        .get(partition)
        );
    }

    public long getNextOffset(String consumerGroupId, String topic, int partition) {
        return readOffset(consumerGroupId, topic, partition)
                .map(offset -> offset + 1)
                .orElse(0L);
    }
}
