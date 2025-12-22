package com.example.kafkaclone.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.grpc.autoconfigure.server.GrpcServerProperties;
import org.springframework.stereotype.Component;

@Component
class GrpcPortOverride implements BeanPostProcessor {
    private final BrokerConfig config;
    private static final Logger log = LoggerFactory.getLogger(GrpcPortOverride.class);

    public GrpcPortOverride(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        if (bean instanceof GrpcServerProperties) {
            GrpcServerProperties props = (GrpcServerProperties) bean;

            if(config.getPort() != null) {
                log.info("Using port: {}", config.getPort());
                props.setPort(config.getPort());
            }
        }
        return bean;
    }
}
