package com.dhimantalapatra.demoapp;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer2 extends Consumer {

  public static final String ID = "consumer2";

  public Consumer2(MessageStore messageStore) {
    super(messageStore, ID);
  }

  @KafkaListener(id = ID, topics = "${app.kafka.topic}", groupId = "springboot-group-2", autoStartup = "true")
  public void listen(String value) {
    receive(value);
  }
}
