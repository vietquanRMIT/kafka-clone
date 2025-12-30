package com.example.kafkaclone.service;

import com.example.kafkaclone.storage.PersistentOffsetStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
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
    private static final String DELIMITER = "|";

    // FLATTENED: Key is "consumerGroup|topic|partition"
    private final Map<String, Long> offsetCache = new ConcurrentHashMap<>();
    private final PersistentOffsetStore persistentOffsetStore;

    public OffsetManager(PersistentOffsetStore persistentOffsetStore) {
        this.persistentOffsetStore = persistentOffsetStore;
    }

    @PostConstruct
    public void init() {
        Map<String, Long> diskOffsets = persistentOffsetStore.recover();
        offsetCache.putAll(diskOffsets);
        logger.info("Recovered {} committed offsets from persistent store", diskOffsets.size());
    }

    public void commitOffset(String consumerGroupId, String topic, int partition, long offset) {
        String key = buildKey(consumerGroupId, topic, partition);
        offsetCache.put(key, offset);
        persistentOffsetStore.commit(key, offset);
    }

    public Optional<Long> readOffset(String consumerGroupId, String topic, int partition) {
        try {
            String key = buildKey(consumerGroupId, topic, partition);
            logger.info("Reading offset for key {}", key);
            if (!offsetCache.containsKey(key)) {
                return Optional.empty();
            }
        } catch (Exception e) {
            logger.error("Error reading offset for group {}, topic {}, partition {}: {}", consumerGroupId, topic, partition, e.getMessage());
            return Optional.empty();
        }
        return Optional.of(offsetCache.get(buildKey(consumerGroupId, topic, partition)));
    }

    public long getNextOffset(String consumerGroupId, String topic, int partition) {
        String key = buildKey(consumerGroupId, topic, partition);
        Long lastCommitted = offsetCache.get(key);
        return (lastCommitted != null) ? lastCommitted + 1 : 0L;
    }

    private String buildKey(String group, String topic, int partition) {
        return group + DELIMITER + topic + DELIMITER + partition;
    }

    @Scheduled(fixedRate = 60000)
    public void diskPersistence() {
        persistentOffsetStore.saveSnapshot(offsetCache);
    }
}
