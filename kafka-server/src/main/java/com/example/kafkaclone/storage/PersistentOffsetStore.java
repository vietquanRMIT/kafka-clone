package com.example.kafkaclone.storage;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Append-only transaction log for consumer offsets.
 */
@Component
public class PersistentOffsetStore {

    private static final Logger logger = LoggerFactory.getLogger(PersistentOffsetStore.class);
    private static final String DATA_DIRECTORY = "./data";
    private static final String FILE_NAME = "committed_offsets.log";
    private static final String TEMP_FILE_NAME = "committed_offsets.log.tmp";
    private static final String DELIMITER = "|";

    private final Path logPath;
    private final Path tempPath;

    private final Lock writeLock = new ReentrantLock();
    private BufferedWriter writer;

    public PersistentOffsetStore() {
        try {
            Path dataDir = Paths.get(DATA_DIRECTORY);
            Files.createDirectories(dataDir);

            this.logPath = dataDir.resolve(FILE_NAME);
            this.tempPath = dataDir.resolve(TEMP_FILE_NAME);

            initWriter();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize PersistentOffsetStore", e);
        }
    }

    public Map<String, Long> recover() {
        if (Files.notExists(logPath)) {
            return new HashMap<>();
        }

        Map<String, Long> tempOffsets = new ConcurrentHashMap<>();
        try (var reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                parseLine(line, tempOffsets);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to recover offsets from " + logPath, e);
        }

        return tempOffsets;
    }

    public void commit(String key, long offset) {
        writeLock.lock();
        try {
            writer.write(key + DELIMITER + offset);
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append offset for key " + key, e);
        } finally {
            writeLock.unlock();
        }
    }

    private void parseLine(String line, Map<String, Long> map) {
        if (line == null || line.isBlank()) return;

        int lastPipeIndex = line.lastIndexOf(DELIMITER);
        if (lastPipeIndex == -1) return;

        String key = line.substring(0, lastPipeIndex);
        String offsetStr = line.substring(lastPipeIndex + 1);

        try {
            map.put(key, Long.parseLong(offsetStr));
        } catch (NumberFormatException ignored) {
            // Malformed line, skip
        }
    }

    /**
     * "Log Compaction" / Snapshot.
     * Replaces the huge log file with a tiny file containing only the latest offsets.
     */
    public void saveSnapshot(Map<String, Long> currentOffsets) {
        writeLock.lock();
        try {
            // 1. Close the current appender
            if (writer != null) {
                writer.flush();
                writer.close();
            }

            // 2. Write ALL current offsets to a temporary file
            try (BufferedWriter snapshotWriter = Files.newBufferedWriter(
                    tempPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                for (Map.Entry<String, Long> entry : currentOffsets.entrySet()) {
                    snapshotWriter.write(entry.getKey() + DELIMITER + entry.getValue());
                    snapshotWriter.newLine();
                }
            }

            // 3. Atomic Move: Replace old log with new snapshot
            Files.move(tempPath, logPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // 4. Re-open the main writer for future appends
            initWriter();

            logger.info("Compacted offset log. Saved {} entries.", currentOffsets.size());

        } catch (IOException e) {
            logger.error("Failed to save offset snapshot", e);
            // Try to re-open writer if snapshot failed, so we don't crash the app
            try { initWriter(); } catch (IOException ignored) {}
        } finally {
            writeLock.unlock();
        }
    }

    private void initWriter() throws IOException {
        this.writer = Files.newBufferedWriter(
                logPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE
        );
    }

    @PreDestroy
    public void close() {
        writeLock.lock();
        try {
            if (writer != null) {
                writer.flush(); // Ensure data is safe on shutdown
                writer.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close PersistentOffsetStore", e);
        } finally {
            writeLock.unlock();
        }
    }
}
