package com.example.kafkaclone.service;

import com.example.kafkaclone.storage.PersistentOffsetStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks committed offsets per consumer group and persists them to disk.
 */
@Component
public class OffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);
    private static final String KEY_DELIMITER = "|";

    // consumerGroup -> topic -> partition -> offset
    private final Map<String, Map<String, Map<Integer, Long>>> committedOffsets = new ConcurrentHashMap<>();
    private final PersistentOffsetStore persistentOffsetStore;

    public OffsetManager(PersistentOffsetStore persistentOffsetStore) {
        this.persistentOffsetStore = persistentOffsetStore;
    }

    @PostConstruct
    public void init() {
        logger.info("Recovering committed offsets from disk");
        persistentOffsetStore.recover().forEach((compositeKey, offset) -> {
            ParsedKey key = parseCompositeKey(compositeKey);
            if (key == null) {
                logger.warn("Skipping malformed offset key {}", compositeKey);
                return;
            }
            committedOffsets
                    .computeIfAbsent(key.consumerGroupId, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(key.topic, k -> new ConcurrentHashMap<>())
                    .put(key.partition, offset);
        });
    }

    public void commitOffset(String consumerGroupId, String topic, int partition, long offset) {
        committedOffsets
                .computeIfAbsent(consumerGroupId, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, key -> new ConcurrentHashMap<>())
                .put(partition, offset);

        persistentOffsetStore.commit(buildCompositeKey(consumerGroupId, topic, partition), offset);
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

    private String buildCompositeKey(String consumerGroupId, String topic, int partition) {
        return consumerGroupId + KEY_DELIMITER + topic + KEY_DELIMITER + partition;
    }

    private ParsedKey parseCompositeKey(String compositeKey) {
        if (compositeKey == null || compositeKey.isBlank()) {
            return null;
        }

        String[] parts = compositeKey.split("\\|", 3);
        if (parts.length != 3) {
            return null;
        }

        int partition;
        try {
            partition = Integer.parseInt(parts[2]);
        } catch (NumberFormatException ex) {
            return null;
        }

        return new ParsedKey(parts[0], parts[1], partition);
    }

    private record ParsedKey(String consumerGroupId, String topic, int partition) {
    }
}
