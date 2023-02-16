package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exceptipn.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {
    // tạo luồng dữ liệu giả
    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    // buil văn bản tweet có đọ dài ngẫu nhiên
    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "congue",
            "massa",
            "sed",
            "qulyinar",
            "ultricies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };
    // tạo tweet
    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData configData,
                                 TwitterKafkaStatusListener statusListener){
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterKafkaStatusListener = statusListener;
    }
    @Override
    public void start() throws Exception {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLenght = twitterToKafkaServiceConfigData.getMockMinTweetLenght();
        int maxTweetLenght = twitterToKafkaServiceConfigData.getMockMaxTweetLenght();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        SimulateTwitterStream(keywords, minTweetLenght, maxTweetLenght, sleepTimeMs);
    }

    private void SimulateTwitterStream(String[] keywords, int minTweetLenght, int maxTweetLenght, long sleepTimeMs){
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLenght, maxTweetLenght);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            }
            catch (TwitterException e) {
               LOG.error("Error creating twitter status!", e);
            }
        });
    }

    private String getFormattedTweet(String[] keywords, int minTweetLenght, int maxTweetLenght) {
        // tạo mảng chuỗi các tham số của đối tượng Tweet
        String[] params = new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)), // lấy độ dài ngẫu nhiên của tweet
                getRandomTweetContext(keywords, minTweetLenght, maxTweetLenght), // hàm tạo nội dung tweet
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)) // lấy id ngẫu nhiên của tweet
        };
        return formatTweetAsJsonWithParams(params);
    }

    // convert list tweet from json
    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for(int i = 0; i < params.length; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    // hàm tạo nội dung 1 tweet
    private String getRandomTweetContext(String[] keywords, int minTweetLenght, int maxTweetLenght) {
        StringBuilder tweet = new StringBuilder();
        int tweetLenght = RANDOM.nextInt(maxTweetLenght - minTweetLenght + 1) + minTweetLenght;
        for (int i = 0; i < tweetLenght; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLenght/2){
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }

    private void sleep(long sleepTimeMs){
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e){
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!");
        }
    }
}
