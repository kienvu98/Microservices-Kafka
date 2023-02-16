package com.microservices.demo.kafka.producer.config.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;


@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> template) {
        this.kafkaTemplate = template;
    }

    // hàm gửi dữ liệu va topic
    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.info("Sending message='{}' to topic= '{}'", message, topicName);
        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture =
                kafkaTemplate.send(topicName, key, message);
        kafkaResultFuture.whenComplete(new BiConsumer<SendResult<Long, TwitterAvroModel>, Throwable>() {
            @Override
            public void accept(SendResult<Long, TwitterAvroModel> longTwitterAvroModelSendResult, Throwable throwable) {
                RecordMetadata metadata = longTwitterAvroModelSendResult.getRecordMetadata();
                LOG.info("Recevid new metadata. Topic: {}; Partition {}; Offset {}; Timestamp {}, at time {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime());
            }
        });
    }

    @PreDestroy
    public void close(){
        if (kafkaTemplate != null){
            LOG.info("Closing kafka producer!");
            kafkaTemplate.destroy();
        }
    }
}
