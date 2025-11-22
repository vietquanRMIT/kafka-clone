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

    @Option(names = {"-p", "--partition"}, required = true, description = "Partition number")
    int partition;

    @Option(names = {"-m", "--message"}, required = true, description = "Message payload")
    String message;

    @Override
    public void run() {
        try (KafkaClient client = new KafkaClient("localhost", 9090)) {
            client.produce(topic, partition, message);
            System.out.println("✅ Message sent to topic " + topic);
        } catch (Exception e) {
            logger.error("Failed to produce message to topic {}: {}", topic, e.getMessage());
        }
    }
}
