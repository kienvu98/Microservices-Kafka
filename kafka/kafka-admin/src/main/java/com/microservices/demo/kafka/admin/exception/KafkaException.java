package com.microservices.demo.kafka.admin.exception;

public class KafkaException extends RuntimeException{
    public KafkaException(){

    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaException(String message) {
        super(message);
    }
}
