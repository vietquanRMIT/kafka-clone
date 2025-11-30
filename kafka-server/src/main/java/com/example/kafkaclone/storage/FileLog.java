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

/**
 * File-backed log for a single topic partition.
 */
public class FileLog {

    private final Path logPath;
    private final FileChannel channel;
    private long nextOffset;

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
            this.channel.position(this.channel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Failed to initialize log for %s-%d", topic, partitionId), e);
        }
    }

    public synchronized long getNextOffset() {
        return nextOffset;
    }

    public synchronized void append(Record record) {
        byte[] payload = record.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + payload.length);
        buffer.putInt(payload.length);
        buffer.put(payload);
        buffer.flip();

        try {
            channel.position(channel.size());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            nextOffset++;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append record to " + logPath, e);
        }
    }

    public synchronized List<Record> read(long offset) {
        List<Record> records = new ArrayList<>();
        try {
            channel.position(0);
            long currentOffset = 0;
            ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);

            while (true) {
                sizeBuffer.clear();
                int readSize = fillBuffer(sizeBuffer);
                if (readSize == -1) {
                    break;
                }

                sizeBuffer.flip();
                int payloadSize = sizeBuffer.getInt();
                if (payloadSize < 0) {
                    throw new IOException("Invalid payload size " + payloadSize + " in " + logPath);
                }

                ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize);
                fillBuffer(payloadBuffer);
                byte[] payload = new byte[payloadSize];
                payloadBuffer.flip();
                payloadBuffer.get(payload);

                if (currentOffset >= offset) {
                    records.add(Record.parseFrom(payload));
                }
                currentOffset++;
            }
            channel.position(channel.size());
            return records;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse record from " + logPath, e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read log " + logPath, e);
        }
    }

    private int fillBuffer(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                if (buffer.position() == 0) {
                    return -1;
                }
                throw new EOFException("Unexpected end of file for " + logPath);
            }
        }
        return buffer.position();
    }

    private long rebuildOffsets() throws IOException {
        channel.position(0);
        long offset = 0;
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);

        while (true) {
            sizeBuffer.clear();
            int readSize = fillBuffer(sizeBuffer);
            if (readSize == -1) {
                break;
            }

            sizeBuffer.flip();
            int payloadSize = sizeBuffer.getInt();
            if (payloadSize < 0) {
                throw new IOException("Invalid payload size " + payloadSize + " in " + logPath);
            }

            long skipped = channel.position() + payloadSize;
            if (skipped > channel.size()) {
                throw new EOFException("Partial record payload encountered in " + logPath);
            }
            channel.position(skipped);
            offset++;
        }

        return offset;
    }

    public synchronized void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close log " + logPath, e);
        }
    }
}
