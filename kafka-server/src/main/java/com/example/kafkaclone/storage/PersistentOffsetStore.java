package com.example.kafkaclone.storage;

import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Append-only transaction log for consumer offsets.
 */
@Component
public class PersistentOffsetStore {

    private static final String DATA_DIRECTORY = "./data";
    private static final String FILE_NAME = "committed_offsets.log";
    private static final String DELIMITER = "|";

    private final Path logPath;
    private final ConcurrentHashMap<String, Long> offsets = new ConcurrentHashMap<>();
    private final Lock writeLock = new ReentrantLock();
    private final BufferedWriter writer;

    public PersistentOffsetStore() {
        try {
            Path dataDir = Paths.get(DATA_DIRECTORY);
            Files.createDirectories(dataDir);
            this.logPath = dataDir.resolve(FILE_NAME);
            this.writer = Files.newBufferedWriter(
                    logPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            );
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize PersistentOffsetStore", e);
        }
    }

    public ConcurrentHashMap<String, Long> recover() {
        if (Files.notExists(logPath)) {
            return offsets;
        }

        try (var reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                parseLine(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to recover offsets from " + logPath, e);
        }

        return offsets;
    }

    public void commit(String compositeKey, long offset) {
        offsets.put(compositeKey, offset);
        String line = compositeKey + DELIMITER + offset + System.lineSeparator();

        writeLock.lock();
        try {
            writer.write(line);
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append offset for key " + compositeKey, e);
        } finally {
            writeLock.unlock();
        }
    }

    private void parseLine(String line) {
        if (line == null || line.isBlank()) {
            return;
        }
        String[] parts = line.split("\\|");
        if (parts.length != 4) {
            return;
        }
        String compositeKey = String.join(DELIMITER, parts[0], parts[1], parts[2]);
        try {
            long parsedOffset = Long.parseLong(parts[3]);
            offsets.put(compositeKey, parsedOffset);
        } catch (NumberFormatException ignored) {
            // skip malformed entries
        }
    }

    @PreDestroy
    public void close() {
        writeLock.lock();
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close PersistentOffsetStore", e);
        } finally {
            writeLock.unlock();
        }
    }
}
