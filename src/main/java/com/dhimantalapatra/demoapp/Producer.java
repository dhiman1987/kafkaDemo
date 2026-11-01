package com.dhimantalapatra.demoapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class Producer {

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String topic;

  public Producer(KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.kafka.topic}") String topic) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
  }

  public CompletableFuture<SendResult<String, String>> sendMessage(String key, String value) {
    return kafkaTemplate.send(topic, key, value)
        .whenComplete((result, ex) -> {
          if (ex != null) {
            logger.error("Failed to produce event to topic {}: key = {}", topic, key, ex);
          } else {
            logger.info("Produced event to topic {}: key = {}, value = {}", topic, key, value);
          }
        });
  }
}