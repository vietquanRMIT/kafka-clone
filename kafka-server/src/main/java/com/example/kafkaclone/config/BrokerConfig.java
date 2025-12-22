package com.example.kafkaclone.config;

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

    private Integer port;

    public String getStorageDir () {
        String storageDir = "./data";
        return storageDir + "/broker-" + id;
    }
}
