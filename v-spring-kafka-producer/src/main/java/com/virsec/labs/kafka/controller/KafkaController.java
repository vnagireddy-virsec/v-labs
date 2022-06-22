package com.virsec.labs.kafka.controller;

import com.virsec.labs.kafka.core.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

    private final Producer producer;

    @Autowired
    KafkaController(Producer producer) {
        this.producer = producer;
    }

    @GetMapping(value = "/publish/{count}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String sendMessageToKafkaTopic(@PathVariable int count) {
        for (int i=1; i<=count; i++) {
            this.producer.sendMessage("message ... " + i);
        }

        return "{\"message\": \"Sent [" + count + "] messages.\"}";
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
        this.producer.sendMessage(message);
    }
}