package com.example.kafkaclient.cmd.command;

import com.example.kafka.api.Record;
import com.example.kafkaclient.cmd.client.KafkaConsumerClient;
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

    private volatile boolean running = true;

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n Shutting down consumer...");
            running = false; // This breaks the loop
        }));

        try (KafkaConsumerClient client = new KafkaConsumerClient(consumerGroupId, partition, topic)) {
            while(running) {
                try {
                    Record record = client.consume();

                    if (record == null) {
                        Thread.sleep(3000); // This will stop thread from fetching for 1s (alternative to long polling)
                        continue;
                    }

                    System.out.println("Message: " + record.getValue().toStringUtf8() + ", Offset: " + record.getOffset());
                } catch (InterruptedException e) {
                    logger.error("Error during poll (will retry): {}", e.getMessage());
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        } catch (Exception e) {
            logger.error("Fatal error starting client", e);
        }
    }
}
