package com.dhimantalapatra.demoapp;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

enum MessageRecipient {
  CONSUMER1("consumer1"),
  CONSUMER2("consumer2"),
  ALL("all");

  static final String HEADER_NAME = "consumer-target";

  private final String value;

  MessageRecipient(String value) {
    this.value = value;
  }

  String value() {
    return value;
  }

  boolean includes(String consumerId) {
    return this == ALL || value.equals(consumerId);
  }

  static Optional<MessageRecipient> fromValue(String value) {
    return Arrays.stream(values())
        .filter(recipient -> recipient.value.equals(value))
        .findFirst();
  }

  static Optional<MessageRecipient> fromHeader(byte[] value) {
    return value == null
        ? Optional.empty()
        : fromValue(new String(value, StandardCharsets.UTF_8));
  }
}
