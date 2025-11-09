package com.example.kafkaclone.cmd;

import com.example.kafkaclone.cmd.command.ProducerCommand;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@CommandLine.Command(
        name = "kafka",
        subcommands = {
                ProducerCommand.class,
        },
        description = "CLI to interact with Kafka"
)
@Component
public class KafkaCli {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new KafkaCli()).execute(args);
        System.exit(exitCode);
    }
}
