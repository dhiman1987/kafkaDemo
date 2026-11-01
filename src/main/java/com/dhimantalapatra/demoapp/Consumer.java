package com.dhimantalapatra.demoapp;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware;

public abstract class Consumer implements ConsumerSeekAware {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final MessageStore messageStore;
  private final String id;
  private final ConcurrentMap<TopicPartition, ConsumerSeekCallback> seekCallbacks = new ConcurrentHashMap<>();
  private final ThreadLocal<ConsumerSeekCallback> callbackForThread = new ThreadLocal<>();

  protected Consumer(MessageStore messageStore, String id) {
    this.messageStore = messageStore;
    this.id = id;
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

  public final void rewindToBeginning() {
    Map<TopicPartition, ConsumerSeekCallback> callbacks = Map.copyOf(seekCallbacks);
    if (callbacks.isEmpty()) {
      throw new IllegalStateException("Kafka consumer has no assigned partitions to rewind");
    }

    messageStore.clear(id);
    callbacks.forEach((topicPartition, callback) ->
        callback.seekToBeginning(topicPartition.topic(), topicPartition.partition()));
    logger.info("Requested Kafka replay from the beginning of {} partitions for {}", callbacks.size(), id);
  }

  public final List<String> getMessages() {
    return messageStore.getMessages(id);
  }

  protected final void receive(String value) {
    logger.info("Received message {}", value);
    messageStore.add(id, value);
    logger.info("Consumed event: value = {}", value);
  }
}
