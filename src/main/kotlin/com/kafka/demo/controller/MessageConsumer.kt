package com.kafka.demo.controller

import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class MessageConsumer {

    @KafkaListener(topics= ["output"], groupId = "test_id")
    fun consume(message:String) :Unit {
        println(" message received from topic : $message");
    }
}