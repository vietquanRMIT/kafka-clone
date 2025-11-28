package com.example.kafkaclone.service;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@GrpcService
public class KafkaService extends KafkaGrpc.KafkaImplBase {

    private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private static final int DEFAULT_PARTITION_ID = 0;

    // topic -> partition -> records
    private final Map<String, Map<Integer, List<Record>>> logs = new ConcurrentHashMap<>();
    private final OffsetManager offsetManager;

    public KafkaService(OffsetManager offsetManager) {
        this.offsetManager = offsetManager;
    }

    @Override
    public void produce(ProducerRequest request, StreamObserver<ProducerResponse> responseObserver) {
        // log -> handle request -> create new Record -> response current offset
        logger.info("Producer received request {}", request.toString());

        String topic = request.getTopic();
        int partition = DEFAULT_PARTITION_ID;
        byte[] value = request.getValue().toByteArray();

        List<Record> records = getOrCreatePartition(topic, partition);
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
        int partition = DEFAULT_PARTITION_ID;
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
        Map<Integer, List<Record>> partitions = new ConcurrentHashMap<>();
        partitions.put(DEFAULT_PARTITION_ID, Collections.synchronizedList(new ArrayList<>()));
        Map<Integer, List<Record>> existing = logs.putIfAbsent(topic, partitions);
        boolean alreadyExists = existing != null;
        if (alreadyExists) {
            existing.computeIfAbsent(DEFAULT_PARTITION_ID, ignored -> Collections.synchronizedList(new ArrayList<>()));
        }

        CreateTopicResponse response = CreateTopicResponse.newBuilder()
                .setErrorCode(alreadyExists ? ErrorCode.TOPIC_ALREADY_EXISTS : ErrorCode.OK)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private List<Record> getOrCreatePartition(String topic, int partition) {
        Map<Integer, List<Record>> partitions = logs.computeIfAbsent(topic, t -> {
            Map<Integer, List<Record>> newPartitions = new ConcurrentHashMap<>();
            newPartitions.put(DEFAULT_PARTITION_ID, Collections.synchronizedList(new ArrayList<>()));
            return newPartitions;
        });

        return partitions.computeIfAbsent(partition, ignored -> Collections.synchronizedList(new ArrayList<>()));
    }

    private List<Record> getPartition(String topic, int partition) {
        Map<Integer, List<Record>> partitions = logs.get(topic);
        if (partitions == null) {
            return null;
        }
        return partitions.get(partition);
    }
}
