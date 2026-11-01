package com.dhimantalapatra.demoapp;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Service;

@Service
public class MessageStore {

  private final ConcurrentMap<String, ConcurrentLinkedDeque<String>> messages = new ConcurrentHashMap<>();

  public void add(String consumerId, String message) {
    messages.computeIfAbsent(consumerId, key -> new ConcurrentLinkedDeque<>()).addFirst(message);
  }

  public void clear(String consumerId) {
    messages.computeIfAbsent(consumerId, key -> new ConcurrentLinkedDeque<>()).clear();
  }

  public List<String> getMessages(String consumerId) {
    ConcurrentLinkedDeque<String> consumerMessages = messages.get(consumerId);
    return consumerMessages == null ? List.of() : List.copyOf(consumerMessages);
  }
}
