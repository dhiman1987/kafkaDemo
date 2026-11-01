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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class MessageController {

  private final Producer producer;
  private final Consumer1 consumer1;
  private final Consumer2 consumer2;

  public MessageController(Producer producer, Consumer1 consumer1, Consumer2 consumer2) {
    this.producer = producer;
    this.consumer1 = consumer1;
    this.consumer2 = consumer2;
  }

  @GetMapping("/")
  public String index(Model model) {
    model.addAttribute("consumer1Messages", consumer1.getMessages());
    model.addAttribute("consumer2Messages", consumer2.getMessages());
    return "index";
  }

  @GetMapping("/messages/fragment")
  public String messageList(Model model) {
    return messageFragment(model, consumer1, Consumer1.ID, "Consumer 1", "/messages/fragment", "/messages/replay", false);
  }

  @GetMapping("/messages/consumer2/fragment")
  public String consumer2MessageList(Model model) {
    return messageFragment(model, consumer2, Consumer2.ID, "Consumer 2",
        "/messages/consumer2/fragment", "/messages/consumer2/replay", false);
  }

  @PostMapping("/messages/replay")
  public String replayMessages(Model model) {
    consumer1.rewindToBeginning();
    return messageFragment(model, consumer1, Consumer1.ID, "Consumer 1", "/messages/fragment", "/messages/replay", true);
  }

  @PostMapping("/messages/consumer2/replay")
  public String replayConsumer2Messages(Model model) {
    consumer2.rewindToBeginning();
    return messageFragment(model, consumer2, Consumer2.ID, "Consumer 2",
        "/messages/consumer2/fragment", "/messages/consumer2/replay", true);
  }

  @GetMapping("/messages")
  @ResponseBody
  public List<String> messages() {
    return consumer1.getMessages();
  }

  @GetMapping("/messages/consumer2")
  @ResponseBody
  public List<String> consumer2Messages() {
    return consumer2.getMessages();
  }

  @PostMapping(path = "/messages", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
  @ResponseBody
  public CompletableFuture<ResponseEntity<String>> sendMessage(@RequestParam String message) {
    if (message.isBlank()) {
      return CompletableFuture.completedFuture(ResponseEntity.badRequest()
          .contentType(MediaType.TEXT_PLAIN)
          .body("Enter a message before sending."));
    }

    return producer.sendMessage(null, message)
        .thenApply(result -> ResponseEntity.status(HttpStatus.ACCEPTED)
            .contentType(MediaType.TEXT_PLAIN)
            .body("Message sent."));
  }

  private String messageFragment(Model model, Consumer consumer, String consumerId, String consumerName,
      String loadUrl, String replayUrl, boolean replayRequested) {
    model.addAttribute("consumerId", consumerId);
    model.addAttribute("consumerName", consumerName);
    model.addAttribute("messages", consumer.getMessages());
    model.addAttribute("loadUrl", loadUrl);
    model.addAttribute("replayUrl", replayUrl);
    model.addAttribute("replayRequested", replayRequested);
    return "fragments/message-list :: messageList(consumerId=${consumerId}, consumerName=${consumerName}, "
        + "messages=${messages}, loadUrl=${loadUrl}, replayUrl=${replayUrl}, replayRequested=${replayRequested})";
  }
}
