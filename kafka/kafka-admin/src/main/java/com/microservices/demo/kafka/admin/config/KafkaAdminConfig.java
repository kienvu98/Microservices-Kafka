package com.microservices.demo.kafka.admin.config;

import com.microservices.demo.config.KafkaConfigData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.stereotype.Component;

import java.util.Map;

@EnableRetry
@Component
public class KafkaAdminConfig {

    private final KafkaConfigData kafkaConfigData;

    public KafkaAdminConfig(KafkaConfigData configData){
        this.kafkaConfigData = configData;
    }

    // hàm tạo admin client kafka
    @Bean
    public AdminClient adminClient(){
        // dùng admin client để tạo và liệt kê các topic
        return AdminClient.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                        kafkaConfigData.getBootstrapServers()));
    }
}
