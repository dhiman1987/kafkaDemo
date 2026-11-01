package com.dhimantalapatra.demoapp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
public class DemoappApplication {

  private static final Logger logger = LoggerFactory.getLogger(DemoappApplication.class);

  public static void main(String[] args) {
    if (System.getenv("CK_USERNAME") == null || System.getenv("CK_PASSWORD") == null) {
      var message = "Required environment variables CK_USERNAME and CK_PASSWORD must be set";
      logger.error("Startup failed : {}",message);
      throw new IllegalStateException(message);
    }

	SpringApplication application = new SpringApplication(DemoappApplication.class);
    application.run(args);
  }

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
}
