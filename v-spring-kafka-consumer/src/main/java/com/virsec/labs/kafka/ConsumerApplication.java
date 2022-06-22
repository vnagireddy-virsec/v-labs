package com.virsec.labs.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;


@SpringBootApplication
public class ConsumerApplication {
    public final static String TOPIC_NAME = "keda-topic";
    public final static String GROUP_ID = "keda-group";

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            System.out.println("================================================================================");
            System.out.println("Let's inspect the beans provided by Spring Boot:");
            System.out.println("--------------------------------------------------------------------------------");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println("-- " + beanName);
            }

            System.out.println("--------------------------------------------------------------------------------");

        };
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @KafkaListener(id = GROUP_ID, topics = TOPIC_NAME)
    public void listen(String in) {
        System.out.println("Received [" + in + "] ...");
        try {
            int sleep_for_seconds = 2;
            System.out.println("Sleeping for [" + sleep_for_seconds + "] ...");
            Thread.sleep(sleep_for_seconds * 1000);
            System.out.println("Done sleeping for [" + sleep_for_seconds + "] ...");
        } catch (Exception e) {
        }
        System.out.println("Done processing [" + in + "] ...");
    }

}
