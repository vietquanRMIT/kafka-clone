package com.example.kafka.cluster;

public record BrokerNode(String id, String host, int port) {
    public String addressKey() {
        return host + ":" + port;
    }
}

