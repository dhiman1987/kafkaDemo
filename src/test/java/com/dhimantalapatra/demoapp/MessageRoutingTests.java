package com.dhimantalapatra.demoapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class MessageRoutingTests {

  @Test
  void producerKeepsPayloadAndAddsRecipientHeader() {
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    CompletableFuture<SendResult<String, String>> sendResult = new CompletableFuture<>();
    doReturn(sendResult).when(kafkaTemplate).send(any(ProducerRecord.class));
    Producer producer = new Producer(kafkaTemplate, "test-topic");

    producer.sendMessage("test-key", "plain-text", MessageRecipient.CONSUMER2);

    ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafkaTemplate).send(recordCaptor.capture());
    ProducerRecord<String, String> record = recordCaptor.getValue();
    assertEquals("test-topic", record.topic());
    assertEquals("test-key", record.key());
    assertEquals("plain-text", record.value());
    Header header = record.headers().lastHeader(MessageRecipient.HEADER_NAME);
    assertNotNull(header);
    assertEquals("consumer2", new String(header.value(), StandardCharsets.UTF_8));
  }

  @Test
  void consumersOnlyStoreMessagesAddressedToThemOrAll() {
    MessageStore messageStore = new MessageStore();
    Consumer1 consumer1 = new Consumer1(messageStore);
    Consumer2 consumer2 = new Consumer2(messageStore);

    consumer1.listen("for-one", bytes("consumer1"));
    consumer2.listen("for-one", bytes("consumer1"));
    consumer1.listen("for-two", bytes("consumer2"));
    consumer2.listen("for-two", bytes("consumer2"));
    consumer1.listen("for-all", bytes("all"));
    consumer2.listen("for-all", bytes("all"));
    consumer1.listen("missing-header", null);
    consumer2.listen("invalid-header", bytes("unknown"));

    assertEquals(List.of("for-all", "for-one"), consumer1.getMessages());
    assertEquals(List.of("for-all", "for-two"), consumer2.getMessages());
  }

  @Test
  void controllerGeneratesUuidAndPassesSelectedRecipient() throws Exception {
    Producer producer = mock(Producer.class);
    CompletableFuture<SendResult<String, String>> sendResult = CompletableFuture.completedFuture(null);
    when(producer.sendMessage(anyString(), eq("hello"), eq(MessageRecipient.CONSUMER1))).thenReturn(sendResult);
    MessageStore messageStore = new MessageStore();
    MessageController controller = new MessageController(
        producer, new Consumer1(messageStore), new Consumer2(messageStore));

    controller.sendMessage("hello", "consumer1").get(1, TimeUnit.SECONDS);

    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    verify(producer).sendMessage(keyCaptor.capture(), eq("hello"), eq(MessageRecipient.CONSUMER1));
    assertTrue(keyCaptor.getValue().matches(
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
  }

  private byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
