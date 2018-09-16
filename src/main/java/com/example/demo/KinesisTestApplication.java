package com.example.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.aws.outbound.KinesisMessageHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.MessageHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

@SpringBootApplication
public class KinesisTestApplication {

  @RestController
  @RequestMapping("/test")
  public class TestController {

    @Autowired
    TestGateway gateway;

    @PostMapping
    public Mono<String> test(@RequestBody String request) throws Exception {
      Map<String, Object> object = new HashMap<String, Object>();
      object.put("id", UUID.randomUUID());
      object.put("message", request);
      gateway.send(object);
      return Mono.just(object.toString());
    }
  }

  @MessagingGateway(defaultRequestChannel = "inputChannel")
  public interface TestGateway {
    void send(Object message);
  }

  @Bean
  public IntegrationFlow testFlow() {
    return IntegrationFlows.from("inputChannel")
        .log(m -> "received message: " + StringEscapeUtils.escapeJava(m.getPayload().toString()))
        .channel("kinesisSendChannel").get();
  }

  @Bean
  public AmazonKinesisAsync kinesis() {
    return AmazonKinesisAsyncClientBuilder.defaultClient();
  }

  @Bean
  @ServiceActivator(inputChannel = "kinesisSendChannel")
  public MessageHandler kinesisMessageHandler(AmazonKinesisAsync amazonKinesis) {
    KinesisMessageHandler kinesisMessageHandler = new KinesisMessageHandler(amazonKinesis);
    kinesisMessageHandler.setStream("test-stream");
    kinesisMessageHandler.setPartitionKey("1");
    kinesisMessageHandler.setConverter(new TestConverter());
    return kinesisMessageHandler;
  }

  private final class TestConverter implements Converter<Object, byte[]> {

    @Override
    public byte[] convert(Object source) {
      return convertString(convertJson(source));
    }

    private String convertJson(Object source) {
      try {
        return new ObjectMapper().writeValueAsString(source);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
        return "";
      }
    }

    private byte[] convertString(Object source) {
      return (source.toString() + "\n").getBytes();
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(KinesisTestApplication.class, args);
  }
}
