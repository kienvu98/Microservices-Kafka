package com.microservices.demo.twitter.to.kafka.service.runner;

import org.springframework.stereotype.Component;

public interface StreamRunner {
    void start() throws Exception;
}
