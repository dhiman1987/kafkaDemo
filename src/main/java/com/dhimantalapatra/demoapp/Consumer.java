package com.dhimantalapatra.demoapp;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Service;

@Service
public class Consumer implements ConsumerSeekAware {

  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  private final MessageStore messageStore;
  private final ConcurrentMap<TopicPartition, ConsumerSeekCallback> seekCallbacks = new ConcurrentHashMap<>();
  private final ThreadLocal<ConsumerSeekCallback> callbackForThread = new ThreadLocal<>();

  public Consumer(MessageStore messageStore) {
    this.messageStore = messageStore;
  }

  @Override
  public void registerSeekCallback(ConsumerSeekCallback callback) {
    callbackForThread.set(callback);
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    ConsumerSeekCallback threadCallback = callbackForThread.get();
    if (threadCallback == null) {
      throw new IllegalStateException("Kafka seek callback was not registered for the listener thread");
    }
    assignments.keySet().forEach(topicPartition -> seekCallbacks.put(topicPartition, threadCallback));
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    partitions.forEach(seekCallbacks::remove);
    callbackForThread.remove();
  }

  public void rewindToBeginning() {
    Map<TopicPartition, ConsumerSeekCallback> callbacks = Map.copyOf(seekCallbacks);
    if (callbacks.isEmpty()) {
      throw new IllegalStateException("Kafka consumer has no assigned partitions to rewind");
    }

    messageStore.clear();
    callbacks.forEach((topicPartition, callback) ->
        callback.seekToBeginning(topicPartition.topic(), topicPartition.partition()));
    logger.info("Requested Kafka replay from the beginning of {} partitions", callbacks.size());
  }

  @KafkaListener(id = "myConsumer", topics = "${app.kafka.topic}", groupId = "springboot-group-1", autoStartup = "true")
  public void listen(String value) {
    logger.info("Received message {}",value);
    messageStore.add(value);
    logger.info("Consumed event: value = {}", value);
  }
}