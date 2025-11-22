package com.example.kafkaclient;

import com.example.kafkaclient.cmd.KafkaCli;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import picocli.CommandLine;

@SpringBootApplication
public class KafkaClientApplication implements CommandLineRunner {

    private final KafkaCli kafkaCli;
    private final ApplicationContext applicationContext;

    public KafkaClientApplication(KafkaCli kafkaCli, ApplicationContext applicationContext) {
        this.kafkaCli = kafkaCli;
        this.applicationContext = applicationContext;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaClientApplication.class, args);
    }

    @Override
    public void run(String... args) {
        CommandLine.IFactory factory = new CommandLine.IFactory() {
            @Override
            public <K> K create(Class<K> cls) throws Exception {
                return applicationContext.getBean(cls);
            }
        };

        CommandLine commandLine = new CommandLine(kafkaCli, factory);
        int exitCode = commandLine.execute(args);
        SpringApplication.exit(applicationContext, () -> exitCode);
        System.exit(exitCode);
    }
}
