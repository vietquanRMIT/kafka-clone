package com.example.kafkaclient.cmd.client;

import com.example.kafka.api.ProducerRequest;
import com.example.kafka.cluster.BrokerNode;

public record PendingProduce(BrokerNode broker, ProducerRequest request) {
}
