package com.microservices.demo.kafka.admin.client;
import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exception.KafkaException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData configData,
                            RetryConfigData retryData,
                            AdminClient admin,
                            RetryTemplate template,
                            WebClient client) {
        this.kafkaConfigData = configData;
        this.retryConfigData = retryData;
        this.adminClient = admin;
        this.retryTemplate = template;
        this.webClient = client;
    }

    // method create topic
    public void createTopics(){
        CreateTopicsResult createTopicsResult;
        // using retryTemplated để gọi phương tức mới để tạo chủ đề
        try{
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t){
            throw new RuntimeException("Reached max number of retry for creating kafka topic(s)!", t);
        }
        checkTopicsCreateed();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Creating {} topics(s), attempt{}", topicNames.size(), retryContext.getRetryCount());

        // stream topic
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());
        return adminClient.createTopics(kafkaTopics);
    }

    // hàm kiểm tra topic  đc tạo
    public void checkTopicsCreateed() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        // seen  create topics
        for (String topic: kafkaConfigData.getTopicNamesToCreate()){
            while(!isTopicCreated(topics, topic)){ // check topic create?
                checkMaxRetry(retryCount++, maxRetry); // ta mức tối đa chưa
                slepp(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    private void slepp(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e){
            throw new KafkaException("Reached max number of retry for creating kafka topic(s)!");
        }
    }

    // hàm kểm tra số lần thử hiện ta có lớn hơn số lần tối đa không
    private void checkMaxRetry(int retry, Integer maxRetry) {
        if (retry > maxRetry){
            throw new KafkaException("Reached max number of retry for creating kafka topic(s)!");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if (topics == null){
            return false;
        }
        return  topics.stream().anyMatch(topic -> topic.equals(topicName));
    }

    // hàm trả về danh sách topic
    private Collection<TopicListing> getTopics(){
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);
        }catch (Throwable e){
            throw new RuntimeException("Reached max number of retry for creating kafka topic(s)!", e);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext)
                     throws ExecutionException, InterruptedException {
        LOG.info("Creating {} topics(s), attempt{}", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if (topics != null){
            topics.forEach(topic -> LOG.debug("Topic with name {}", topic.name()));
        }
        return topics;
    }

    // hàm kiểm tra schemaRegistry có hoạt động không
    public void checkSchemaRegistry(){
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while(!getSchemaRegistryStatus().is2xxSuccessful()){
            checkMaxRetry(retryCount++, maxRetry); // ta mức tối đa chưa
            slepp(sleepTimeMs);
            sleepTimeMs *= multiplier;
            //topics = getTopics();
        }
    }

    // hàm trả về trạng thái shemaRegistry tetry
    private HttpStatus getSchemaRegistryStatus(){
        try{
            return (HttpStatus) webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e){
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }
}
