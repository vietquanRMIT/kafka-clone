package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.CommitOffsetRequest;
import com.example.kafka.api.ConsumerRequest;
import com.example.kafka.api.ConsumerResponse;
import com.example.kafka.api.FetchOffsetRequest;
import com.example.kafka.api.FetchOffsetResponse;
import com.example.kafka.api.KafkaGrpc;
import com.example.kafka.api.ProducerRequest;
import com.example.kafka.api.ProducerResponse;
import com.example.kafka.api.Record;
import com.google.protobuf.ByteString;
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

    public void produce(String topic, int partition, String message, String key) {
        ProducerRequest request = ProducerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setValue(ByteString.copyFromUtf8(message))
                .setKey(key)
                .build();

        ProducerResponse response = stub.produce(request);
        System.out.println("Offset: " + response.getOffset());
    }

    public Record consume(String topic, int partition, long offset) {
        ConsumerRequest request = ConsumerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .build();

        ConsumerResponse response = stub.consume(request);
        return response.getRecord();
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
        CommitOffsetRequest request = CommitOffsetRequest.newBuilder()
                .setConsumerGroupId(consumerGroupId)
                .setTopic(topic)
                .setPartition(partition)
                .setOffset(offset)
                .build();

        stub.commitOffset(request);
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
