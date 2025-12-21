package com.example.kafkaclone.service;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka.broker")
@Getter
@Setter
public class BrokerConfig {
    private String id;

    private int port;

    public String getStorageDir () {
        String storageDir = "./data";
        return storageDir + "/broker-" + id;
    }
}
