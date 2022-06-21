package com.virsec.labs.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;


@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
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
        return TopicBuilder.name("topic1")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
        return args -> {
            String message_count = System.getenv("V_MESSAGE_COUNT");
            int default_count = 10;
            int count = (null == message_count || message_count.isBlank() || message_count.isEmpty()) ? default_count : Integer.parseInt(message_count);
            for (int i=0; i < count; i++) {
                template.send("topic1", "test" + i);
            }
        };
    }

}
