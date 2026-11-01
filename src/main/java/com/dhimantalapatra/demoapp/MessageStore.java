package com.dhimantalapatra.demoapp;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.stereotype.Service;

@Service
public class MessageStore {

  private final ConcurrentLinkedDeque<String> messages = new ConcurrentLinkedDeque<>();

  public void add(String message) {
    messages.addFirst(message);
  }

  public void clear() {
    messages.clear();
  }

  public List<String> getMessages() {
    return List.copyOf(messages);
  }
}
