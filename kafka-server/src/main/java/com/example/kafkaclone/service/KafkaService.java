package com.example.kafkaclone.service;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@GrpcService
@RequiredArgsConstructor
public class KafkaService extends KafkaGrpc.KafkaImplBase {

    private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private static final int DEFAULT_PARTITION_COUNT = 3;

    // topic -> partition -> records
    private final Map<String, Map<Integer, List<Record>>> logs = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> topicRoundRobinCounters = new ConcurrentHashMap<>();
    private final OffsetManager offsetManager;

    @Override
    public void produce(ProducerRequest request, StreamObserver<ProducerResponse> responseObserver) {
        // log -> handle request -> create new Record -> response current offset
        logger.info("Producer received request {}", request.toString());

        String topic = request.getTopic();
        byte[] value = request.getValue().toByteArray();
        String key = request.getKey().isEmpty() ? null : request.getKey();

        Map<Integer, List<Record>> partitions = ensureTopic(topic, DEFAULT_PARTITION_COUNT);
        int partitionCount = partitions.size();
        int partition = calculatePartition(topic, key, partitionCount);
        List<Record> records = partitions.get(partition);
        Record newRecord = Record.newBuilder()
                .setOffset(records.size())
                .setValue(ByteString.copyFrom(value))
                .build();

        records.add(newRecord);

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

        List<Record> records = getPartition(topic, partition);
        if (records == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(String.format("topic %s and partition %s not found", topic, partition)).asRuntimeException());
            return;
        }

        if (offset < 0 || offset >= records.size()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(String.format("offset %s not found", offset)).asRuntimeException());
            return;
        }

        Record record = records.get((int) offset);

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
        Map<Integer, List<Record>> newPartitions = initializePartitions(requestedPartitions);
        Map<Integer, List<Record>> existing = logs.putIfAbsent(topic, newPartitions);
        boolean alreadyExists = existing != null;

        if (alreadyExists) {
            ensurePartitionEntries(existing, requestedPartitions);
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

    private List<Record> getPartition(String topic, int partition) {
        Map<Integer, List<Record>> partitions = logs.get(topic);
        if (partitions == null) {
            return null;
        }
        return partitions.get(partition);
    }

    private Map<Integer, List<Record>> ensureTopic(String topic, int numPartitions) {
        int partitionsToEnsure = numPartitions <= 0 ? DEFAULT_PARTITION_COUNT : numPartitions;
        Map<Integer, List<Record>> partitions = logs.computeIfAbsent(topic, t -> {
            topicRoundRobinCounters.putIfAbsent(t, new AtomicInteger(0));
            return initializePartitions(partitionsToEnsure);
        });
        topicRoundRobinCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
        ensurePartitionEntries(partitions, partitionsToEnsure);
        return partitions;
    }

    private Map<Integer, List<Record>> initializePartitions(int numPartitions) {
        int partitionCount = numPartitions <= 0 ? DEFAULT_PARTITION_COUNT : numPartitions;
        Map<Integer, List<Record>> partitions = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.put(i, Collections.synchronizedList(new ArrayList<>()));
        }
        return partitions;
    }

    private void ensurePartitionEntries(Map<Integer, List<Record>> partitions, int numPartitions) {
        for (int i = 0; i < numPartitions; i++) {
            partitions.computeIfAbsent(i, ignored -> Collections.synchronizedList(new ArrayList<>()));
        }
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
}
