package com.example.kafkaclient.cmd.command;

import com.example.kafkaclient.cmd.client.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "produce", description = "Send a message to a topic partition")
@Component
public class ProducerCommand implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ProducerCommand.class);

    @Option(names = {"-t", "--topic"}, required = true, description = "Topic name")
    String topic;

    @Option(names = {"-p", "--partition"}, required = false, description = "Partition number. Omit to let the broker assign via default partitioner")
    Integer partition;

    @Option(names = {"-m", "--message"}, required = true, description = "Message payload")
    String message;

    @Option(names = {"-k", "--key"}, required = false, description = "Message key")
    String key;

    @Option(names = {"--port"}, description = "Broker port", defaultValue = "9091")
    int port;

    @Override
    public void run() {
        try (KafkaClient client = new KafkaClient("localhost", port)) {
            client.produce(topic, partition, message, key);
            System.out.println("✅ Message sent to topic " + topic);
        } catch (Exception e) {
            logger.error("Failed to produce message to topic {}: {}", topic, e.getMessage());
        }
    }
}
