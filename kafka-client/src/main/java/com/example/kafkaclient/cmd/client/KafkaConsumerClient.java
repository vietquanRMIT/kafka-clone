package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.example.kafka.cluster.BrokerNode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dedicated client responsible for consuming records.
 */
public class KafkaConsumerClient extends AbstractKafkaClient {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class);
    private List<Record> recordCache = new ArrayList<>();

    private final String consumerGroupId;
    private final int partition;
    private final String topic;

    private final AtomicLong currentOffset;
    private long lastCommittedOffset;

    public KafkaConsumerClient(String consumerGroupId, int partition, String topic) {
        this.consumerGroupId = consumerGroupId;
        this.partition = partition;
        this.topic = topic;

        long startOffset = fetchCommittedOffset(consumerGroupId, topic, partition);
        this.currentOffset = new AtomicLong(startOffset);
        this.lastCommittedOffset = startOffset;

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::autoCommitOffset, 3000, 3000, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public Record consume() {
        Record cachedRecord = findInCache(currentOffset.get());
        if (cachedRecord != null) {
            currentOffset.incrementAndGet();
            return cachedRecord;
        }

        ConsumerRequest request = ConsumerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(currentOffset.get())
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);

        try {
            ConsumerResponse response = stub.consume(request);
            this.recordCache = response.getRecordsList();

            if (this.recordCache.isEmpty()) {
                return null;
            }

            Record firstRecord = recordCache.getFirst();
            currentOffset.set(firstRecord.getOffset() + 1);

            return firstRecord;
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

    private long fetchCommittedOffset(String consumerGroupId, String topic, int partition) {
        if (consumerGroupId == null || consumerGroupId.isBlank()) {
            return 0L;
        }

        FetchOffsetRequest request = FetchOffsetRequest.newBuilder()
                .setConsumerGroupId(nonNull(consumerGroupId))
                .setTopic(topic)
                .setPartition(partition)
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);
        FetchOffsetResponse response = stub.fetchOffset(request);

        return response.getOffset();
    }

    private void autoCommitOffset() {
        if (consumerGroupId == null || consumerGroupId.isBlank()) {
            return;
        }

        if (currentOffset.get() <= lastCommittedOffset) {
            logger.info("currentOffset {} is less than or equal to lastCommittedOffset {}", currentOffset, lastCommittedOffset);
            return;
        }

        forceCommit();
    }

    private void forceCommit() {
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
                .setConsumerGroupId(consumerGroupId)
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(currentOffset.get())
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);

        try {
            CommitOffsetResponse commitOffsetResponse = stub.commitOffset(request);
            lastCommittedOffset = currentOffset.get();
            logger.info("Committed offset {} for {}-{}", currentOffset, topic, partition);
        } catch (Exception e) {
            logger.error("Failed to commit offset for {}-{}", topic, partition, e);
        }
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

    private String nonNull(String groupId) {
        return groupId == null ? "" : groupId;
    }

    @Override
    public void close() {
        forceCommit();
        super.close();
    }
}
