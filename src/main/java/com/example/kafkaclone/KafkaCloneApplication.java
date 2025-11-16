package com.example.kafkaclone;

import com.example.kafkaclone.cmd.KafkaCli;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import picocli.CommandLine;

@SpringBootApplication
public class KafkaCloneApplication implements CommandLineRunner {

    @Autowired
    private KafkaCli kafkaCli;

    @Autowired
    private ApplicationContext applicationContext;

    public static void main(String[] args) {
        SpringApplication.run(KafkaCloneApplication.class, args);
    }

    @Override
    public void run(String... args) {
        // Only execute CLI commands if arguments are provided
        if (args.length > 0) {
            CommandLine commandLine = new CommandLine(kafkaCli, new CommandLine.IFactory() {
                @Override
                public <K> K create(Class<K> cls) throws Exception {
                    return applicationContext.getBean(cls);
                }
            });
            int exitCode = commandLine.execute(args);
            System.exit(exitCode);
        }
        // Otherwise, just run as a server (do nothing, let Spring Boot continue)
    }
}
