package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.example.kafka.cluster.ClusterMetadata;
import com.example.kafka.cluster.ClusterMetadata.BrokerNode;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaClient implements AutoCloseable {
//   Logger
    private final Logger logger = LoggerFactory.getLogger(KafkaClient.class);

//   Message queue and executor for producing messages
    private final BlockingQueue<PendingProduce> messageQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

//   gRPC channel pool
    private final ConcurrentHashMap<String, ManagedChannel> channelPool = new ConcurrentHashMap<>();

//   Partition chooser and offset commit tracking
    private final AtomicInteger partitionChooser = new AtomicInteger();
    private long lastCommitTime = 0;
    private static final long AUTO_COMMIT_INTERVAL_MS = 3000;
    private List<Record> recordCache = new ArrayList<>();

    public KafkaClient() {
        executor.submit(this::sendLoop);
    }

    public void produce(String topic, Integer partition, String message, String key) {
        int targetPartition = partition != null ? partition : selectPartition(topic);
        BrokerNode leader = requireLeader(topic, targetPartition);

        ProducerRequest.Builder builder = ProducerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(targetPartition)
                .setValue(ByteString.copyFromUtf8(message));

        if (key != null) {
            builder.setKey(key);
        }

        ProducerRequest request = builder.build();
        boolean success = messageQueue.offer(new PendingProduce(leader, request));
        if (!success) {
            logger.error("Queue is full! Message dropped for topic {}", topic);
        }
    }

    public Record consume(String consumerGroupId, String topic, int partition, long offset) {
        Record cachedRecord = findInCache(offset);
        if (cachedRecord != null) {
            logger.info("Found cached record");
            commitOffset(consumerGroupId, topic, partition, offset);
            return cachedRecord;
        }

        ConsumerRequest request = ConsumerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);

        try {
            ConsumerResponse response = stub.consume(request);
            this.recordCache = response.getRecordsList();

            if (this.recordCache.isEmpty()) {
                return null;
            }

            commitOffset(consumerGroupId, topic, partition, offset);
            return this.recordCache.getFirst();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
                logger.warn("Invalid consume request for topic {} partition {}: {}", topic, partition, e.getMessage());
            } else {
                logger.error("gRPC error consuming from {}-{}: {}", topic, partition, e.getMessage());
            }
            return null;
        } catch (Exception e) {
            logger.error("Failed to consume from {}-{}: {}", topic, partition, e.getMessage());
            return null;
        }
    }

    public long fetchCommittedOffset(String consumerGroupId, String topic, int partition) {
        FetchOffsetRequest request = FetchOffsetRequest.newBuilder()
                .setConsumerGroupId(consumerGroupId)
                .setTopic(topic)
                .setPartition(partition)
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);
        FetchOffsetResponse response = stub.fetchOffset(request);
        return response.getOffset();
    }

    private void commitOffset(String consumerGroupId, String topic, int partition, long offset) {
        if (consumerGroupId == null || consumerGroupId.isEmpty()) {
            return;
        }

        if (System.currentTimeMillis() - lastCommitTime < AUTO_COMMIT_INTERVAL_MS) {
            return;
        }

        forceCommit(consumerGroupId, topic, partition, offset);
    }

    private Record findInCache(long targetOffset) {
        if (recordCache == null || recordCache.isEmpty()) {
            return null;
        }
        long first = recordCache.getFirst().getOffset();
        long last = recordCache.getLast().getOffset();

        if (targetOffset >= first && targetOffset <= last) {
            for (Record r : recordCache) {
                if (r.getOffset() == targetOffset) {
                    return r;
                }
            }
        }
        return null;
    }

    private void forceCommit(String groupId, String topic, int partition, long offset) {
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
                .setConsumerGroupId(groupId)
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);

        try {
            stub.commitOffset(request);
            this.lastCommitTime = System.currentTimeMillis();
            logger.info("Auto-committed offset {} for {}-{}", offset, topic, partition);
        } catch (Exception e) {
            logger.error("Failed to auto-commit", e);
        }
    }

    private KafkaGrpc.KafkaBlockingStub getStub(BrokerNode brokerNode) {
        ManagedChannel channel = channelPool.computeIfAbsent(brokerNode.addressKey(), key -> ManagedChannelBuilder
                .forAddress(brokerNode.host(), brokerNode.port())
                .usePlaintext()
                .build());
        return KafkaGrpc.newBlockingStub(channel);
    }

    private BrokerNode requireLeader(String topic, int partition) {
        BrokerNode leader = ClusterMetadata.getLeader(topic, partition);
        if (leader == null) {
            throw new IllegalArgumentException("No leader configured for topic %s partition %d".formatted(topic, partition));
        }
        return leader;
    }

    private int selectPartition(String topic) {
        int partitionCount = ClusterMetadata.getPartitionCount(topic);
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Topic %s has no configured partitions".formatted(topic));
        }
        return Math.floorMod(partitionChooser.getAndIncrement(), partitionCount);
    }

    private void sendLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                PendingProduce pendingProduce = messageQueue.take();
                KafkaGrpc.KafkaBlockingStub stub = getStub(pendingProduce.broker());
                ProducerResponse response = stub.produce(pendingProduce.request());
                logger.info("Sent message with offset: {}", response.getOffset());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Failed to send message", e);
            }
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            boolean status = executor.awaitTermination(5, TimeUnit.SECONDS);
            if (status) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        channelPool.forEach((key, channel) -> {
            if (!channel.isShutdown()) {
                channel.shutdown();
            }
        });
        channelPool.clear();
    }

    private record PendingProduce(BrokerNode broker, ProducerRequest request) {
    }
}
