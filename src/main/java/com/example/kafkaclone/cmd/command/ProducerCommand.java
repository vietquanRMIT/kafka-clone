package com.example.kafkaclone.cmd.command;

import com.example.kafkaclone.cmd.client.KafkaClient;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Option;
import picocli.CommandLine.Command;

@Command(
        name = "produce", description = "Send message"
)
@Component
public class ProducerCommand implements Runnable {

    @Option(names = {"-t", "--topic"}, required = true, description = "Topic name")
    String topic;

    @Option(names = {"-p", "--partition"}, required = true, description = "Partition number")
    int partition;

    @Option(names = {"-m", "--message"}, required = true, description = "Message to send")
    String message;

    @Override
    public void run() {
        try (KafkaClient client = new KafkaClient("localhost", 9090)) {
            client.produce(topic, partition, message);
            System.out.println("✅ Message sent to topic " + topic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
