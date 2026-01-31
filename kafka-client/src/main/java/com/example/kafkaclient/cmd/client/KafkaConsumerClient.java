package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.CommitOffsetRequest;
import com.example.kafka.api.ConsumerRequest;
import com.example.kafka.api.ConsumerResponse;
import com.example.kafka.api.FetchOffsetRequest;
import com.example.kafka.api.FetchOffsetResponse;
import com.example.kafka.api.KafkaGrpc;
import com.example.kafka.api.Record;
import com.example.kafka.cluster.BrokerNode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Dedicated client responsible for consuming records.
 */
public class KafkaConsumerClient extends AbstractKafkaClient {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class);
    private List<Record> recordCache = new ArrayList<>();

    public Record consume(String consumerGroupId, String topic, int partition, long offset) {
        Record cachedRecord = findInCache(offset);
        if (cachedRecord != null) {
            maybeCommit(consumerGroupId, topic, partition, cachedRecord.getOffset());
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

            if (this.recordCache == null || this.recordCache.isEmpty()) {
                return null;
            }

            Record record = this.recordCache.getFirst();
            maybeCommit(consumerGroupId, topic, partition, record.getOffset());
            return record;
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
                .setConsumerGroupId(nonNull(consumerGroupId))
                .setTopic(topic)
                .setPartition(partition)
                .build();

        BrokerNode leader = requireLeader(topic, partition);
        KafkaGrpc.KafkaBlockingStub stub = getStub(leader);
        FetchOffsetResponse response = stub.fetchOffset(request);
        return response.getOffset();
    }

    private void maybeCommit(String consumerGroupId, String topic, int partition, long offset) {
        if (consumerGroupId == null || consumerGroupId.isBlank()) {
            return;
        }
        forceCommit(consumerGroupId, topic, partition, offset);
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
            logger.info("Committed offset {} for {}-{}", offset, topic, partition);
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
}
