package com.example.kafkaclone;

import com.example.kafkaclone.service.BrokerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.grpc.autoconfigure.server.GrpcServerProperties;

@SpringBootApplication
public class KafkaCloneApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaCloneApplication.class, args);
    }

    // THIS IS THE HACK
    @Bean
    public GrpcServerPropertiesOverride propertiesOverride(BrokerConfig brokerConfig) {
        return new GrpcServerPropertiesOverride(brokerConfig);
    }

    static class GrpcServerPropertiesOverride implements org.springframework.beans.factory.config.BeanPostProcessor {
        private final BrokerConfig config;

        public GrpcServerPropertiesOverride(BrokerConfig config) {
            this.config = config;
        }

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) {
            if (bean instanceof GrpcServerProperties) {
                GrpcServerProperties props = (GrpcServerProperties) bean;
                // MANUALLY overwrite the port with our BrokerConfig value
                System.out.println("🔨 [Force Fix] Overwriting gRPC Port to: " + config.getPort());
                props.setPort(config.getPort());
            }
            return bean;
        }
    }
}
