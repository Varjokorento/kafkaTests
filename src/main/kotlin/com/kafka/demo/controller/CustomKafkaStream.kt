package com.kafka.demo.controller

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced

class CustomKafkaStream {
    companion object {
        fun build(): Topology {
            val resultTopic = "output"
            val topicList = arrayListOf<String>("values", "values_one", "values_two")

            val streamsBuilder = StreamsBuilder()

            val allStream: KStream<String, String> =
                streamsBuilder
                .stream(topicList as Collection<String>?, Consumed.with(Serdes.String(), Serdes.String()))

            val resStream: KStream<String, String> = allStream.map { _, p ->
                KeyValue("data", p + "_output")
            }

            resStream.to(resultTopic, Produced.with(Serdes.String(), Serdes.String()))
            return streamsBuilder.build()
        }
    }
}