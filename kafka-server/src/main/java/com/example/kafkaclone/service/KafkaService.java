package com.example.kafkaclone.service;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.example.kafka.cluster.ClusterMetadata;
import com.example.kafka.cluster.ClusterMetadata.BrokerNode;
import com.example.kafkaclone.config.BrokerConfig;
import com.example.kafkaclone.storage.FileLog;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@GrpcService
public class KafkaService extends KafkaGrpc.KafkaImplBase {

    private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

    private final BrokerConfig brokerConfig;
    private final String brokerId;

    // topic -> partition -> log
    private final Map<String, Map<Integer, FileLog>> logs = new ConcurrentHashMap<>();
    private final OffsetManager offsetManager;
    private Map<String, List<Integer>> assignedPartitions = Map.of();

    public KafkaService(OffsetManager offsetManager, BrokerConfig brokerConfig) {
        this.offsetManager = offsetManager;
        this.brokerConfig = brokerConfig;
        this.brokerId = brokerConfig.getId();
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
        int partition = request.getPartition();

        if (!ensureLocalLeadership(topic, partition, responseObserver)) {
            return;
        }

        FileLog fileLog = getOrCreatePartition(topic, partition);
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

        if (!ensureLocalLeadership(topic, partition, responseObserver)) {
            return;
        }

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

        ConsumerResponse consumerResponse = ConsumerResponse.newBuilder()
                .addAllRecords(records)
                .build();

        responseObserver.onNext(consumerResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetResponse> responseObserver) {
        logger.info("CommitOffset received request {}", request);

        if (!ensureLocalLeadership(request.getTopic(), request.getPartition(), responseObserver)) {
            return;
        }

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

        if (!ensureLocalLeadership(request.getTopic(), request.getPartition(), responseObserver)) {
            return;
        }

        long lastCommitted = offsetManager.readOffset(
                request.getConsumerGroupId(),
                request.getTopic(),
                request.getPartition()
        ).orElse(0L);

        logger.info("Last committed offset is {}", lastCommitted);

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
        Map<Integer, String> topicAssignments = ClusterMetadata.getPartitionAssignments(topic);

        if (topicAssignments.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Topic %s is not defined in cluster metadata".formatted(topic))
                    .asRuntimeException());
            return;
        }

        Map<Integer, FileLog> topicLogs = logs.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
        topicAssignments.forEach((partitionId, leaderId) -> {
            if (brokerId.equals(leaderId)) {
                topicLogs.computeIfAbsent(partitionId, pid -> createFileLog(topic, pid));
            }
        });

        CreateTopicResponse response = CreateTopicResponse.newBuilder()
                .setErrorCode(ErrorCode.OK)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private FileLog createFileLog(String topic, int partitionId) {
        return new FileLog(brokerConfig.getStorageDir(), topic, partitionId);
    }

    private FileLog getPartition(String topic, int partition) {
        Map<Integer, FileLog> partitions = logs.get(topic);
        if (partitions == null) {
            return null;
        }
        return partitions.get(partition);
    }

    @PostConstruct
    public void init() {
        logger.info("KafkaService initialized for broker {}", brokerId);
        this.assignedPartitions = ClusterMetadata.getAssignmentsForBroker(brokerId);
        initializeAssignedPartitions();
    }

    private void initializeAssignedPartitions() {
        if (assignedPartitions.isEmpty()) {
            logger.warn("No partitions assigned to broker {} via cluster metadata", brokerId);
            return;
        }

        assignedPartitions.forEach((topic, partitions) -> {
            Map<Integer, FileLog> topicLogs = logs.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
            for (Integer partition : partitions) {
                topicLogs.computeIfAbsent(partition, pid -> createFileLog(topic, pid));
            }
        });
    }

    private FileLog getOrCreatePartition(String topic, int partitionId) {
        return logs
                .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(partitionId, pid -> createFileLog(topic, pid));
    }

    private boolean ensureLocalLeadership(String topic, int partition, StreamObserver<?> responseObserver) {
        if (partition < 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Partition must be non-negative for topic %s".formatted(topic))
                    .asRuntimeException());
            return false;
        }

        BrokerNode leader = ClusterMetadata.getLeader(topic, partition);
        if (leader == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("No leader configured for %s-%d".formatted(topic, partition))
                    .asRuntimeException());
            return false;
        }

        if (!brokerId.equals(leader.id())) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Broker %s is not leader for %s-%d".formatted(brokerId, topic, partition))
                    .asRuntimeException());
            return false;
        }

        return true;
    }
}
