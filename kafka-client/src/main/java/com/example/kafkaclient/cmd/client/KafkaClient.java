package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.*;
import com.example.kafka.api.Record;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final KafkaGrpc.KafkaBlockingStub stub;
    private final Logger logger = LoggerFactory.getLogger(KafkaClient.class);

    private long lastCommitTime = 0;
    private static final long AUTO_COMMIT_INTERVAL_MS = 5000;

    private List<Record> recordCache = new ArrayList<>();

    public KafkaClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = KafkaGrpc.newBlockingStub(channel);
    }

    public void produce(String topic, Integer partition, String message, String key) {
        ProducerRequest.Builder builder = ProducerRequest.newBuilder()
                .setTopic(topic)
                .setValue(ByteString.copyFromUtf8(message));

        if (partition != null) {
            builder.setPartition(partition);
        }
        if (key != null) {
            builder.setKey(key);
        }

        ProducerRequest request = builder.build();

        ProducerResponse response = stub.produce(request);
        System.out.println("Offset: " + response.getOffset());
    }

    public Record consume(String topic, int partition, long offset) {
        Record cachedRecord = findInCache(offset);
        if (cachedRecord != null) {
            logger.info("Found cached record");
            return cachedRecord;
        }

        ConsumerRequest request = ConsumerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .build();

        ConsumerResponse response = stub.consume(request);

        // Update cache
        this.recordCache = response.getRecordsList();

        if(this.recordCache.isEmpty()) {
            throw new RuntimeException("No records returned from broker");
        }
        return this.recordCache.getFirst();
    }

    public long fetchCommittedOffset(String consumerGroupId, String topic, int partition) {
        FetchOffsetRequest request = FetchOffsetRequest.newBuilder()
                .setConsumerGroupId(consumerGroupId)
                .setTopic(topic)
                .setPartition(partition)
                .build();

        FetchOffsetResponse response = stub.fetchOffset(request);
        return response.getOffset();
    }

    public void commitOffset(String consumerGroupId, String topic, int partition, long offset) {
        if (System.currentTimeMillis() - lastCommitTime < AUTO_COMMIT_INTERVAL_MS) {
            // It has been less than 5 seconds. Do nothing.
            return;
        }

        forceCommit(consumerGroupId, topic, partition, offset);
    }

    // Helper to search the buffer
    private Record findInCache(long targetOffset) {
        if (recordCache == null || recordCache.isEmpty()) {
            return null;
        }
        // Optimization: Check if target is within the range of our buffer
        long first = recordCache.getFirst().getOffset();
        long last = recordCache.getLast().getOffset();

        if (targetOffset >= first && targetOffset <= last) {
            // It's in range! Find it. (Simple linear search for now)
            for (Record r : recordCache) {
                if (r.getOffset() == targetOffset) {
                    return r;
                }
            }
        }
        return null;
    }

    // Helper method for the actual Network Call
    private void forceCommit(String groupId, String topic, int partition, long offset) {
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
                .setConsumerGroupId(groupId)
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .build();

        try {
            CommitOffsetResponse commitOffsetResponse = stub.commitOffset(request);
            this.lastCommitTime = System.currentTimeMillis(); // Reset timer

            logger.info("✅ Auto-committed offset: " + offset);
        } catch (Exception e) {
            logger.error("Failed to auto-commit", e);
        }
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
