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

	//private final Producer producer;

  public static void main(String[] args) {
    if (System.getenv("CK_USERNAME") == null || System.getenv("CK_PASSWORD") == null) {
      throw new IllegalStateException("Required environment variables CK_USERNAME and CK_PASSWORD must be set");
    }

	SpringApplication application = new SpringApplication(DemoappApplication.class);
    //application.setWebApplicationType(WebApplicationType.NONE);
    application.run(args);
  }

 /*@Bean
  public CommandLineRunner CommandLineRunnerBean() {
    return (args) -> {
      logger.info("Starting Kafka listener startup. Registered listener IDs: {}",
          kafkaListenerEndpointRegistry.getListenerContainerIds());

      logger.info("Sending startup test message");
      this.producer.sendMessage("key", "value");
      MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("myConsumer");
      if (listenerContainer == null) {
        logger.error("Kafka listener container 'myConsumer' was not found");
        throw new IllegalStateException("Kafka listener container 'myConsumer' was not found");
      }

      logger.info("Starting Kafka listener container '{}'", listenerContainer.getListenerId());
      try {
        listenerContainer.start();
        logger.info("Kafka listener container '{}' started; running={}",
            listenerContainer.getListenerId(), listenerContainer.isRunning());
      } catch (RuntimeException exception) {
        logger.error("Failed to start Kafka listener container '{}'",
            listenerContainer.getListenerId(), exception);
        throw exception;
      }
    };
  }
  @Autowired
  DemoappApplication(Producer producer) {
    this.producer = producer;
  }*/


  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
}
