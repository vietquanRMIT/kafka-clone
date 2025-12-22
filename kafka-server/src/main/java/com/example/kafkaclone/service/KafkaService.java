package com.example.kafkaclone.service;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.example.kafkaclone.config.BrokerConfig;
import com.example.kafkaclone.storage.FileLog;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@GrpcService
public class KafkaService extends KafkaGrpc.KafkaImplBase {

    private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private static final int DEFAULT_PARTITION_COUNT = 3;

    private final BrokerConfig brokerConfig;

    // topic -> partition -> log
    private final Map<String, Map<Integer, FileLog>> logs = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> topicRoundRobinCounters = new ConcurrentHashMap<>();
    private final OffsetManager offsetManager;

    public KafkaService(OffsetManager offsetManager, BrokerConfig brokerConfig) {
        this.offsetManager = offsetManager;
        this.brokerConfig = brokerConfig;
    }
    @PostConstruct
    public void logConfig() {
        System.out.println("Broker ID: " + brokerConfig.getId());
        System.out.println("Broker Port: " + brokerConfig.getPort());
        System.out.println("Storage Dir: " + brokerConfig.getStorageDir());
    }
    @Override
    public void produce(ProducerRequest request, StreamObserver<ProducerResponse> responseObserver) {
        // log -> handle request -> create new Record -> response current offset
        logger.info("Producer received request {}", request.toString());

        String topic = request.getTopic();
        byte[] value = request.getValue().toByteArray();
        String key = request.getKey().isEmpty() ? null : request.getKey();

        Map<Integer, FileLog> partitions = ensureTopic(topic, DEFAULT_PARTITION_COUNT);
        int partitionCount = partitions.size();
        int partition = calculatePartition(topic, key, partitionCount);
        FileLog fileLog = partitions.get(partition);
        long offset = fileLog.getNextOffset();

        Record newRecord = Record.newBuilder()
                .setOffset(offset)
                .setValue(ByteString.copyFrom(value))
                .build();

        fileLog.append(newRecord);

        ProducerResponse response = ProducerResponse.newBuilder()
                .setOffset(newRecord.getOffset())
                .setError(ErrorCode.OK)
                .setLeaderAddress("localhost:5001")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void consume(ConsumerRequest request, StreamObserver<ConsumerResponse> responseObserver) {
        logger.info("Consumer received request {}", request.toString());

        String topic = request.getTopic();
        int partition = request.getPartition();
        long offset = request.getOffset();

        FileLog fileLog = getPartition(topic, partition);
        if (fileLog == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(String.format("topic %s and partition %s not found", topic, partition)).asRuntimeException());
            return;
        }

        if (offset < 0 || offset >= fileLog.getNextOffset()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(String.format("offset %s not found", offset)).asRuntimeException());
            return;
        }

        List<Record> records = fileLog.read(offset, 100);
        if (records.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(String.format("offset %s not found", offset)).asRuntimeException());
            return;
        }

        Record record = records.getFirst();

        ConsumerResponse consumerResponse = ConsumerResponse.newBuilder()
                        .setRecord(record)
                                .build();

        responseObserver.onNext(consumerResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetResponse> responseObserver) {
        logger.info("CommitOffset received request {}", request);

        offsetManager.commitOffset(
                request.getConsumerGroupId(),
                request.getTopic(),
                request.getPartition(),
                request.getOffset()
        );

        responseObserver.onNext(CommitOffsetResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void fetchOffset(FetchOffsetRequest request, StreamObserver<FetchOffsetResponse> responseObserver) {
        logger.info("FetchOffset received request {}", request);

        long lastCommitted = offsetManager.readOffset(
                request.getConsumerGroupId(),
                request.getTopic(),
                request.getPartition()
        ).orElse(-1L);

        FetchOffsetResponse response = FetchOffsetResponse.newBuilder()
                .setOffset(lastCommitted)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createTopic(CreateTopicRequest request, StreamObserver<CreateTopicResponse> responseObserver) {
        logger.info("CreateTopic received request {}", request);

        String topic = request.getTopic();
        int requestedPartitions = request.getPartitions() > 0 ? request.getPartitions() : DEFAULT_PARTITION_COUNT;
        Map<Integer, FileLog> newPartitions = initializePartitions(topic, requestedPartitions);
        Map<Integer, FileLog> existing = logs.putIfAbsent(topic, newPartitions);
        boolean alreadyExists = existing != null;

        if (alreadyExists) {
            ensurePartitionEntries(topic, existing, requestedPartitions);
            topicRoundRobinCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        } else {
            topicRoundRobinCounters.put(topic, new AtomicInteger(0));
        }

        CreateTopicResponse response = CreateTopicResponse.newBuilder()
                .setErrorCode(alreadyExists ? ErrorCode.TOPIC_ALREADY_EXISTS : ErrorCode.OK)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void createTopic(String topic) {
        createTopic(topic, null);
    }

    public void createTopic(String topic, Integer numPartitions) {
        int partitions = (numPartitions == null || numPartitions <= 0) ? DEFAULT_PARTITION_COUNT : numPartitions;
        ensureTopic(topic, partitions);
    }

    private FileLog getPartition(String topic, int partition) {
        Map<Integer, FileLog> partitions = logs.get(topic);
        if (partitions == null) {
            return null;
        }
        return partitions.get(partition);
    }

    private Map<Integer, FileLog> ensureTopic(String topic, int numPartitions) {
        int partitionsToEnsure = numPartitions <= 0 ? DEFAULT_PARTITION_COUNT : numPartitions;
        Map<Integer, FileLog> partitions = logs.computeIfAbsent(topic, t -> {
            topicRoundRobinCounters.putIfAbsent(t, new AtomicInteger(0));
            return initializePartitions(t, partitionsToEnsure);
        });
        topicRoundRobinCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        ensurePartitionEntries(topic, partitions, partitionsToEnsure);
        return partitions;
    }

    private Map<Integer, FileLog> initializePartitions(String topic, int numPartitions) {
        int partitionCount = numPartitions <= 0 ? DEFAULT_PARTITION_COUNT : numPartitions;
        Map<Integer, FileLog> partitions = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.put(i, createFileLog(topic, i));
        }
        return partitions;
    }

    private void ensurePartitionEntries(String topic, Map<Integer, FileLog> partitions, int numPartitions) {
        for (int i = 0; i < numPartitions; i++) {
            int partitionId = i;
            partitions.computeIfAbsent(partitionId, ignored -> createFileLog(topic, partitionId));
        }
    }

    private FileLog createFileLog(String topic, int partitionId) {
        return new FileLog(brokerConfig.getStorageDir(), topic, partitionId);
    }

    private int calculatePartition(String topic, String key, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive");
        }
        if (key != null) {
            return Math.floorMod(key.hashCode(), numPartitions);
        }

        AtomicInteger counter = topicRoundRobinCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        return Math.floorMod(counter.getAndIncrement(), numPartitions);
    }

    @PostConstruct
    public void init() {
        logger.info("KafkaService initialized");
        loadExistingLogs();
    }

    private void loadExistingLogs() {
        Path dataDir = Paths.get(brokerConfig.getStorageDir());
        try {
            if (!Files.exists(dataDir)) {
                Files.createDirectories(dataDir);
                return;
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.log")) {
                for (Path path : stream) {
                    String fileName = path.getFileName().toString();
                    if (!fileName.endsWith(".log")) {
                        continue;
                    }

                    String baseName = fileName.substring(0, fileName.length() - 4);
                    int separator = baseName.lastIndexOf('-');
                    if (separator <= 0 || separator == baseName.length() - 1) {
                        logger.warn("Skipping log file with unexpected name: {}", fileName);
                        continue;
                    }

                    String topic = baseName.substring(0, separator);
                    String partitionText = baseName.substring(separator + 1);
                    int partitionId;
                    try {
                        partitionId = Integer.parseInt(partitionText);
                    } catch (NumberFormatException ex) {
                        logger.warn("Skipping log file with non-numeric partition: {}", fileName);
                        continue;
                    }

                    logs.computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                            .computeIfAbsent(partitionId, pid -> createFileLog(topic, pid));
                    topicRoundRobinCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
                }
            }
        } catch (IOException e) {
            logger.error("Failed to load existing log files from {}", brokerConfig.getStorageDir(), e);
        }
    }
}
