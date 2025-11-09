package com.example.kafkaclone;

import com.example.kafkaclone.cmd.KafkaCli;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import picocli.CommandLine;

@SpringBootApplication
public class KafkaCloneApplication implements CommandLineRunner {

    @Autowired
    private KafkaCli kafkaCli;

    public static void main(String[] args) {
        SpringApplication.run(KafkaCloneApplication.class, args);
    }

    @Override
    public void run(String... args) {
        new CommandLine(kafkaCli).execute(args);
    }
}
