package com.example.kafkaclone.storage;

import com.example.kafka.api.Record;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * File-backed log for a single topic partition with sparse indexing.
 */
public class FileLog {

    private static final int INDEX_INTERVAL_BYTES = 4 * 1024; // 4KB checkpoints

    private final Path logPath;
    private final FileChannel channel;
    private final TreeMap<Long, Long> offsetIndex = new TreeMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private long nextOffset;
    private long bytesWrittenSinceLastIndex = 0L;

    public FileLog(String directory, String topic, int partitionId) {
        try {
            Path dir = Paths.get(directory);
            Files.createDirectories(dir);
            this.logPath = dir.resolve(String.format("%s-%d.log", topic, partitionId));
            this.channel = FileChannel.open(
                    this.logPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );

            this.nextOffset = rebuildOffsets();
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Failed to initialize log for %s-%d", topic, partitionId), e);
        }
    }

    public long getNextOffset() {
        rwLock.readLock().lock();
        try {
            return nextOffset;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void append(Record record) {
        rwLock.writeLock().lock();
        try {
            byte[] payload = record.toByteArray();
            long recordStart = channel.size();

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + payload.length);
            buffer.putInt(payload.length);
            buffer.put(payload);
            buffer.flip();

            channel.position(recordStart);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            maybeIndex(record.getOffset(), recordStart, Integer.BYTES + payload.length);
            nextOffset++;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append record to " + logPath, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<Record> read(long offset, int maxRecords) {
        if (maxRecords <= 0) {
            return List.of();
        }

        rwLock.readLock().lock();
        try {
            if (offset < 0 || offset >= nextOffset) {
                return List.of();
            }

            Map.Entry<Long, Long> checkpoint = offsetIndex.floorEntry(offset);
            long currentOffset = checkpoint != null ? checkpoint.getKey() : 0L;
            long pointer = checkpoint != null ? checkpoint.getValue() : 0L;

            List<Record> records = new ArrayList<>(Math.min(maxRecords, 64));
            ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            long fileSize = channel.size();

            while (currentOffset < nextOffset && records.size() < maxRecords && pointer < fileSize) {
                Integer payloadSize = readSizeAt(pointer, sizeBuffer);
                if (payloadSize == null) {
                    break;
                }
                pointer += Integer.BYTES;

                if (payloadSize < 0) {
                    throw new IOException("Invalid payload size " + payloadSize + " in " + logPath);
                }
                if (pointer + payloadSize > fileSize) {
                    throw new EOFException("Partial record payload encountered in " + logPath);
                }

                if (currentOffset < offset) {
                    pointer += payloadSize;
                    currentOffset++;
                    continue;
                }

                ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize);
                readFully(payloadBuffer, pointer);
                pointer += payloadSize;

                byte[] payload = payloadBuffer.array();
                records.add(Record.parseFrom(payload));
                currentOffset++;
            }

            return records;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse record from " + logPath, e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read log " + logPath, e);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private Integer readSizeAt(long position, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int bytesRead = readFully(buffer, position);
        if (bytesRead == -1) {
            return null;
        }
        buffer.flip();
        return buffer.getInt();
    }

    private int readFully(ByteBuffer buffer, long position) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + totalRead);
            if (read == -1) {
                if (totalRead == 0) {
                    return -1;
                }
                throw new EOFException("Unexpected end of file for " + logPath);
            }
            totalRead += read;
        }
        return totalRead;
    }

    private long rebuildOffsets() throws IOException {
        offsetIndex.clear();
        bytesWrittenSinceLastIndex = 0L;
        channel.position(0);

        long offset = 0;
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);

        while (true) {
            long recordStart = channel.position();
            sizeBuffer.clear();
            int readSize = channel.read(sizeBuffer);
            if (readSize == -1) {
                break;
            }
            if (readSize < Integer.BYTES) {
                throw new EOFException("Partial record size encountered in " + logPath);
            }

            sizeBuffer.flip();
            int payloadSize = sizeBuffer.getInt();
            if (payloadSize < 0) {
                throw new IOException("Invalid payload size " + payloadSize + " in " + logPath);
            }

            long payloadEnd = channel.position() + payloadSize;
            if (payloadEnd > channel.size()) {
                throw new EOFException("Partial record payload encountered in " + logPath);
            }

            channel.position(payloadEnd);
            maybeIndex(offset, recordStart, Integer.BYTES + payloadSize);
            offset++;
        }

        return offset;
    }

    public void close() {
        rwLock.writeLock().lock();
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close log " + logPath, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void maybeIndex(long offset, long recordStart, long recordBytes) {
        bytesWrittenSinceLastIndex += recordBytes;
        if (offsetIndex.isEmpty() || bytesWrittenSinceLastIndex >= INDEX_INTERVAL_BYTES) {
            offsetIndex.put(offset, recordStart);
            bytesWrittenSinceLastIndex = 0L;
        }
    }
}
