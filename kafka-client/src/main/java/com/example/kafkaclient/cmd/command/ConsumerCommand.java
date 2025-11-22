package com.example.kafkaclient.cmd.command;

import com.example.kafkaclient.cmd.client.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "consume", description = "Fetch a record from a topic partition")
@Component
public class ConsumerCommand implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerCommand.class);

    @Option(names = {"-t", "--topic"}, required = true, description = "Topic name")
    String topic;

    @Option(names = {"-p", "--partition"}, required = true, description = "Partition number")
    int partition;

    @Option(names = {"-o", "--offset"}, required = true, description = "Offset to read from")
    long offset;

    @Override
    public void run() {
        try (KafkaClient client = new KafkaClient("localhost", 9090)) {
            client.consume(topic, partition, offset);
        } catch (Exception e) {
            logger.error("Failed to consume message from topic {}: {}", topic, e.getMessage());
        }
    }
}
