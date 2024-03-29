package com.microservices.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfigData {
    private List<String> twitterKeywords;
    private String wellcomeMessage;
    private Boolean   enableMockTweets;
    private Integer mockMinTweetLenght;
    private Integer mockMaxTweetLenght;
    private Long mockSleepMs;
}
