package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.KafkaGrpc;
import com.example.kafka.api.ProducerRequest;
import com.example.kafka.api.ProducerResponse;
import com.example.kafka.cluster.BrokerNode;
import com.example.kafka.cluster.ClusterMetadata;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated client responsible for producing records.
 */
public class KafkaProducerClient extends AbstractKafkaClient {

    private final Logger logger = LoggerFactory.getLogger(KafkaProducerClient.class);
    private final BlockingQueue<PendingProduce> messageQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public KafkaProducerClient() {
        executor.submit(this::sendLoop);
    }

    public void produce(String topic, Integer partition, String message, String key) {
        int targetPartition = partition != null ? partition : selectPartition(topic, key);
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

    private int selectPartition(String topic, String key) {
        int partitionCount = ClusterMetadata.getPartitionCount(topic);
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Topic %s has no configured partitions".formatted(topic));
        }
        if (key != null && !key.isEmpty()) {
            return Math.floorMod(key.hashCode(), partitionCount);
        }
        return ThreadLocalRandom.current().nextInt(partitionCount);
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
            if (!status) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.close();
    }
}
