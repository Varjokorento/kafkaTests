package com.kafka.demo.controller

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.streams.KafkaStreams
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import java.util.concurrent.Future

@RestController
class KafkaController {

    @Autowired
    constructor(kafkaTemplate: KafkaTemplate<String, String>) {
        this.kafkaTemplate = kafkaTemplate
        stream()
    }

    var kafkaTemplate: KafkaTemplate<String, String>? = null;

    @GetMapping("/produce")
    fun produceMessage(): ResponseEntity<String> {
        println("here")
        val message = "Vastuullisuus"
        produce(message, "values")
        produce("Yhteisöllisyys", "values_one")
        return produce("Asiakaslähtöisyys", "values_two")
    }

    fun stream() {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["application.id"] = "kafka-tutorial"
        KafkaStreams(CustomKafkaStream.build(), props).start()
    }

    fun produce(message: String, topic: String): ResponseEntity<String> {
        var producerRecord: ProducerRecord<String, String> = ProducerRecord(topic, message)
        val map = mutableMapOf<String, String>()
        map["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        map["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        map["bootstrap.servers"] = "localhost:9092"
        var producer = KafkaProducer<String, String>(map as Map<String, Any>?)
        var future: Future<RecordMetadata> = producer?.send(producerRecord)!!
        return ResponseEntity.ok(" message sent to " + future.get().topic());
    }


}
