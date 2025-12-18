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
 * <p>
 * This class provides persistent storage for Kafka-like records using a file-based log.
 * It implements sparse indexing to enable efficient random access to records by offset.
 * The log is thread-safe, supporting concurrent reads and exclusive writes.
 * </p>
 */
public class FileLog {

    /** Interval in bytes after which a new index checkpoint is created for faster lookups */
    private static final int INDEX_INTERVAL_BYTES = 4 * 1024; // 4KB checkpoints

    /** Path to the log file on disk */
    private final Path logPath;

    /** FileChannel for efficient I/O operations on the log file */
    private final FileChannel channel;

    /** Sparse index mapping offsets to file positions for efficient seek operations */
    private final TreeMap<Long, Long> offsetIndex = new TreeMap<>();

    /** Read-write lock to allow concurrent reads but exclusive writes */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** The next offset to be assigned to an appended record */
    private long nextOffset;

    /** Tracks bytes written since last index checkpoint to determine when to create next checkpoint */
    private long bytesWrittenSinceLastIndex = 0L;

    /**
     * Constructs a FileLog for a specific topic partition.
     * <p>
     * Creates the log directory if it doesn't exist, opens the log file,
     * and rebuilds the offset index from existing data.
     * </p>
     *
     * @param directory    the directory where log files are stored
     * @param topic        the topic name
     * @param partitionId  the partition identifier
     * @throws UncheckedIOException if the log cannot be initialized
     */
    public FileLog(String directory, String topic, int partitionId) {
        try {
            // Create the log directory structure if it doesn't exist
            Path dir = Paths.get(directory);
            Files.createDirectories(dir);

            // Construct log file path with format: topic-partitionId.log
            this.logPath = dir.resolve(String.format("%s-%d.log", topic, partitionId));

            // Open the file channel with create, read, and write permissions
            this.channel = FileChannel.open(
                    this.logPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );

            // Rebuild the offset index from existing log data and set nextOffset
            this.nextOffset = rebuildOffsets();
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Failed to initialize log for %s-%d", topic, partitionId), e);
        }
    }

    /**
     * Returns the next offset that will be assigned to an appended record.
     * <p>
     * This method is thread-safe and uses a read lock to ensure consistent reads
     * while allowing concurrent access from multiple threads.
     * </p>
     *
     * @return the next available offset
     */
    public long getNextOffset() {
        rwLock.readLock().lock();
        try {
            return nextOffset;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Appends a record to the end of the log file.
     * <p>
     * The record is serialized and written with a 4-byte length prefix.
     * This method is thread-safe and uses a write lock to ensure exclusive access
     * during the append operation. The sparse index is updated if necessary.
     * </p>
     *
     * @param record the record to append
     * @throws UncheckedIOException if the append operation fails
     */
    public void append(Record record) {
        rwLock.writeLock().lock();
        try {
            // Serialize the record to bytes
            byte[] payload = record.toByteArray();
            long recordStart = channel.size();

            // Create buffer with format: [4 bytes length][payload bytes]
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + payload.length);
            buffer.putInt(payload.length);
            buffer.put(payload);
            buffer.flip();

            // Position at end of file and write the complete record
            channel.position(recordStart);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            // Update sparse index if we've written enough bytes since last checkpoint
            maybeIndex(record.getOffset(), recordStart, Integer.BYTES + payload.length);
            nextOffset++;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append record to " + logPath, e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Reads a batch of records starting from the specified offset.
     * <p>
     * This method uses the sparse index to efficiently seek to the nearest checkpoint
     * before the requested offset, then sequentially scans forward to find the exact
     * starting position. It is thread-safe and uses a read lock to allow concurrent reads.
     * </p>
     *
     * @param offset     the starting offset to read from (inclusive)
     * @param maxRecords the maximum number of records to read
     * @return a list of records starting from the given offset, up to maxRecords
     * @throws RuntimeException if a record cannot be parsed
     * @throws UncheckedIOException if an I/O error occurs during reading
     */
    public List<Record> read(long offset, int maxRecords) {
        // Early return for invalid maxRecords
        if (maxRecords <= 0) {
            return List.of();
        }

        rwLock.readLock().lock();
        try {
            // Validate offset is within valid range
            if (offset < 0 || offset >= nextOffset) {
                return List.of();
            }

            // Find the nearest index checkpoint at or before the requested offset
            Map.Entry<Long, Long> checkpoint = offsetIndex.floorEntry(offset);
            long currentOffset = checkpoint != null ? checkpoint.getKey() : 0L;
            long pointer = checkpoint != null ? checkpoint.getValue() : 0L;

            // Pre-allocate list with reasonable initial capacity
            List<Record> records = new ArrayList<>(Math.min(maxRecords, 64));
            ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            long fileSize = channel.size();

            // Scan through records from checkpoint to build result list
            while (currentOffset < nextOffset && records.size() < maxRecords && pointer < fileSize) {
                // Read the 4-byte length prefix
                Integer payloadSize = readSizeAt(pointer, sizeBuffer);
                if (payloadSize == null) {
                    break; // Reached end of file
                }
                pointer += Integer.BYTES;

                // Validate payload size
                if (payloadSize < 0) {
                    throw new IOException("Invalid payload size " + payloadSize + " in " + logPath);
                }
                if (pointer + payloadSize > fileSize) {
                    throw new EOFException("Partial record payload encountered in " + logPath);
                }

                // Skip records before the requested offset
                if (currentOffset < offset) {
                    pointer += payloadSize;
                    currentOffset++;
                    continue;
                }

                // Read and deserialize the record payload
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

    /**
     * Reads a 4-byte integer size field at the specified position in the log file.
     * <p>
     * This helper method reads the length prefix that precedes each record in the log.
     * </p>
     *
     * @param position the file position to read from
     * @param buffer   a reusable buffer for reading the size (must be at least 4 bytes)
     * @return the size value, or null if end of file is reached
     * @throws IOException if an I/O error occurs
     */
    private Integer readSizeAt(long position, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int bytesRead = readFully(buffer, position);
        if (bytesRead == -1) {
            return null; // End of file reached
        }
        buffer.flip();
        return buffer.getInt();
    }

    /**
     * Reads data from the file channel into the buffer until the buffer is full.
     * <p>
     * This method handles partial reads by looping until the entire buffer is filled
     * or end of file is reached. It ensures that the requested number of bytes are
     * read or an exception is thrown.
     * </p>
     *
     * @param buffer   the buffer to fill with data
     * @param position the starting file position to read from
     * @return the total number of bytes read, or -1 if end of file is reached immediately
     * @throws EOFException if end of file is encountered before buffer is full
     * @throws IOException if an I/O error occurs
     */
    private int readFully(ByteBuffer buffer, long position) throws IOException {
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + totalRead);
            if (read == -1) {
                if (totalRead == 0) {
                    return -1; // End of file reached at start
                }
                throw new EOFException("Unexpected end of file for " + logPath);
            }
            totalRead += read;
        }
        return totalRead;
    }

    /**
     * Rebuilds the offset index by scanning through the entire log file.
     * <p>
     * This method is called during initialization to reconstruct the sparse index
     * from existing log data. It reads each record's size, validates the data,
     * and creates index checkpoints at regular intervals. This enables efficient
     * random access to records after a restart.
     * </p>
     *
     * @return the next offset to be used (equal to the number of records found)
     * @throws EOFException if the log file contains incomplete records
     * @throws IOException if an I/O error occurs or invalid data is encountered
     */
    private long rebuildOffsets() throws IOException {
        // Reset index state for fresh rebuild
        offsetIndex.clear();
        bytesWrittenSinceLastIndex = 0L;
        channel.position(0);

        long offset = 0;
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);

        // Scan through all records in the log file
        while (true) {
            long recordStart = channel.position();
            sizeBuffer.clear();
            int readSize = channel.read(sizeBuffer);

            // Check if we've reached the end of the file
            if (readSize == -1) {
                break;
            }
            if (readSize < Integer.BYTES) {
                throw new EOFException("Partial record size encountered in " + logPath);
            }

            // Extract and validate the payload size
            sizeBuffer.flip();
            int payloadSize = sizeBuffer.getInt();
            if (payloadSize < 0) {
                throw new IOException("Invalid payload size " + payloadSize + " in " + logPath);
            }

            // Verify that the complete payload exists in the file
            long payloadEnd = channel.position() + payloadSize;
            if (payloadEnd > channel.size()) {
                throw new EOFException("Partial record payload encountered in " + logPath);
            }

            // Skip to the next record and update the index if necessary
            channel.position(payloadEnd);
            maybeIndex(offset, recordStart, Integer.BYTES + payloadSize);
            offset++;
        }

        return offset;
    }

    /**
     * Closes the log file and releases all associated resources.
     * <p>
     * This method flushes any pending writes and closes the underlying file channel.
     * It is thread-safe and uses a write lock to ensure no operations are in progress
     * during closure. After calling this method, no further operations should be
     * performed on this FileLog instance.
     * </p>
     *
     * @throws UncheckedIOException if the file channel cannot be closed
     */
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

    /**
     * Conditionally adds an index checkpoint for the given offset.
     * <p>
     * This method implements sparse indexing by creating checkpoints at regular byte intervals
     * (defined by INDEX_INTERVAL_BYTES). The first record is always indexed, and subsequent
     * records are indexed only after enough bytes have been written since the last checkpoint.
     * This balances memory usage against seek performance.
     * </p>
     *
     * @param offset      the logical offset of the record
     * @param recordStart the file position where the record starts
     * @param recordBytes the total size of the record (including length prefix)
     */
    private void maybeIndex(long offset, long recordStart, long recordBytes) {
        bytesWrittenSinceLastIndex += recordBytes;
        // Index the first record or when we've accumulated enough bytes since last checkpoint
        if (offsetIndex.isEmpty() || bytesWrittenSinceLastIndex >= INDEX_INTERVAL_BYTES) {
            offsetIndex.put(offset, recordStart);
            bytesWrittenSinceLastIndex = 0L;
        }
    }
}
