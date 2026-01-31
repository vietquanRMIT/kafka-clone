package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.KafkaGrpc;
import com.example.kafka.cluster.BrokerNode;
import com.example.kafka.cluster.ClusterMetadata;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared utilities for Kafka gRPC clients.
 */
abstract class AbstractKafkaClient implements AutoCloseable {

    private final ConcurrentHashMap<String, ManagedChannel> channelPool = new ConcurrentHashMap<>();

    protected BrokerNode requireLeader(String topic, int partition) {
        BrokerNode leader = ClusterMetadata.getLeader(topic, partition);
        if (leader == null) {
            throw new IllegalArgumentException("No leader configured for topic %s partition %d".formatted(topic, partition));
        }
        return leader;
    }

    protected KafkaGrpc.KafkaBlockingStub getStub(BrokerNode brokerNode) {
        ManagedChannel channel = channelPool.computeIfAbsent(brokerNode.addressKey(), key -> ManagedChannelBuilder
                .forAddress(brokerNode.host(), brokerNode.port())
                .usePlaintext()
                .build());
        return KafkaGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() {
        channelPool.forEach((key, channel) -> {
            if (!channel.isShutdown()) {
                channel.shutdown();
            }
        });
        channelPool.clear();
    }
}
