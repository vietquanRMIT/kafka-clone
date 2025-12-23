package com.example.kafkaclient.cmd.command;

import com.example.kafka.api.Record;
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

    @Option(names = {"-g", "--group"}, required = false, description = "Consumer group identifier")
    String consumerGroupId;

    @Option(names = {"-o", "--offset"}, required = false, description = "Offset to read from. If omitted, the next committed offset for the provided group will be used.")
    Long offset;

    @Option(names = {"--port"}, description = "Broker port", defaultValue = "9091")
    int port;

    @Override
    public void run() {
        try (KafkaClient client = new KafkaClient("localhost", port)) {
            long effectiveOffset = resolveOffset(client);

            Record record = client.consume(topic, partition, effectiveOffset);
            String message = record.getValue().toStringUtf8();
            System.out.println("Message: " + message);
            System.out.println("Offset: " + record.getOffset());

            if (consumerGroupId != null) {
                client.commitOffset(consumerGroupId, topic, partition, record.getOffset());
                System.out.printf("✅ Committed offset %d for group %s%n", record.getOffset(), consumerGroupId);
            }
        } catch (Exception e) {
            logger.error("Failed to consume message from topic {}: {}", topic, e.getMessage());
        }
    }

    private long resolveOffset(KafkaClient client) {
        if (offset != null) {
            return offset;
        }

        if (consumerGroupId == null) {
            return 0L;
        }

        long committed = client.fetchCommittedOffset(consumerGroupId, topic, partition);
        return committed < 0 ? 0L : committed + 1;
    }
}
