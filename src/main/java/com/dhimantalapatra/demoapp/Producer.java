package com.dhimantalapatra.demoapp;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

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

  public CompletableFuture<SendResult<String, String>> sendMessage(
      String key, String value, MessageRecipient recipient) {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    record.headers().add(MessageRecipient.HEADER_NAME, recipient.value().getBytes(StandardCharsets.UTF_8));

    return kafkaTemplate.send(record)
        .whenComplete((result, ex) -> {
          if (ex != null) {
            logger.error("Failed to produce event to topic {}: key = {}", topic, key, ex);
          } else {
            logger.info("Produced event to topic {}: key = {}, recipient = {}", topic, key, recipient.value());
          }
        });
  }
}