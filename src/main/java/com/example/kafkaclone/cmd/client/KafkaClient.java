package com.example.kafkaclone.cmd.client;

import com.example.kafka.api.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class KafkaClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final KafkaGrpc.KafkaBlockingStub stub;

    public KafkaClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.stub = KafkaGrpc.newBlockingStub(channel);
    }

    public void produce(String topic, int partition, String message) {
        ProducerRequest request = ProducerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setValue(com.google.protobuf.ByteString.copyFromUtf8(message))
                .build();

        ProducerResponse response = stub.produce(request);
        System.out.println("Offset: " + response.getOffset());
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
