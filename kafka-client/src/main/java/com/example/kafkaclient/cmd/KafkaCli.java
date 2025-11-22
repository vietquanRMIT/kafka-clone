package com.example.kafkaclient.cmd;

import com.example.kafkaclient.cmd.command.ConsumerCommand;
import com.example.kafkaclient.cmd.command.ProducerCommand;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@CommandLine.Command(
        name = "kafka",
        description = "CLI for interacting with the Kafka clone broker",
        mixinStandardHelpOptions = true,
        subcommands = {
                ProducerCommand.class,
                ConsumerCommand.class
        }
)
@Component
public class KafkaCli implements Runnable {

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }
}
