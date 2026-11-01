package com.dhimantalapatra.demoapp;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MessageController {

  private final MessageStore messageStore;
  private final Producer producer;
  private final Consumer consumer;

  public MessageController(MessageStore messageStore, Producer producer, Consumer consumer) {
    this.messageStore = messageStore;
    this.producer = producer;
    this.consumer = consumer;
  }

  @GetMapping("/")
  public String index(Model model) {
    model.addAttribute("messages", messageStore.getMessages());
    return "index";
  }

  @GetMapping("/messages/fragment")
  public String messageList(Model model) {
    model.addAttribute("messages", messageStore.getMessages());
    return "fragments/message-list :: messageList";
  }

  @PostMapping("/messages/replay")
  public String replayMessages(Model model) {
    consumer.rewindToBeginning();
    model.addAttribute("messages", messageStore.getMessages());
    model.addAttribute("replayRequested", true);
    return "fragments/message-list :: messageList";
  }

  @GetMapping("/messages")
  @ResponseBody
  public List<String> messages() {
    return messageStore.getMessages();
  }

  @PostMapping(path = "/messages", consumes = MediaType.TEXT_PLAIN_VALUE)
  @ResponseBody
  public CompletableFuture<ResponseEntity<Void>> sendMessage(@RequestBody String message) {
    if (message.isBlank()) {
      return CompletableFuture.completedFuture(ResponseEntity.badRequest().build());
    }

    return producer.sendMessage(null, message)
        .thenApply(result -> ResponseEntity.status(HttpStatus.ACCEPTED).build());
  }
}
