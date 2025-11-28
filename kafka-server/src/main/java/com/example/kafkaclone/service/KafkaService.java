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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@GrpcService
public class KafkaService extends KafkaGrpc.KafkaImplBase {

    private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    // eg: topic { partition: {Record, Record, ...}}
    private final Map<String, Map<Integer, List<Record>>> logs = new HashMap<>();
    private final OffsetManager offsetManager;

    public KafkaService(OffsetManager offsetManager) {
        this.offsetManager = offsetManager;
    }

    @Override
    public void produce(ProducerRequest request, StreamObserver<ProducerResponse> responseObserver) {
        // log -> handle request -> create new Record -> response current offset
        logger.info("Producer received request {}", request.toString());

        String topic = request.getTopic();
        int partition = request.getPartition();
        byte[] value = request.getValue().toByteArray();

        logs.putIfAbsent(topic, new HashMap<>());
        logs.get(topic).putIfAbsent(partition, new ArrayList<>());

        Record newRecord = Record.newBuilder()
                .setOffset(logs.get(topic).get(partition).size())
                .setValue(ByteString.copyFrom(value))
                .build();

        logs.get(topic).get(partition).add(newRecord);

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

        if (!logs.containsKey(topic) | !logs.get(topic).containsKey(partition)) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(String.format("topic %s and partition %s not found", topic, partition)).asRuntimeException());
            return;
        }

        List<Record> records = logs.get(topic).get(partition);

        if (offset < 0 | offset >= records.size()) {
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
        super.createTopic(request, responseObserver);
    }
}
