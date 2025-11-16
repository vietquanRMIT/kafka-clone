package com.example.kafkaclone.cmd;

import com.example.kafkaclone.cmd.command.ProducerCommand;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@CommandLine.Command(
        name = "kafka",
        subcommands = {
                ProducerCommand.class,
        },
        description = "CLI to interact with Kafka",
        mixinStandardHelpOptions = true
)
@Component
public class KafkaCli implements Runnable {
    @Override
    public void run() {
        // Print help if no subcommand is provided
        new CommandLine(this).usage(System.out);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new KafkaCli()).execute(args);
        System.exit(exitCode);
    }
}
