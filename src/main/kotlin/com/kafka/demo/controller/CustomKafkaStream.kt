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
            val valueTopic = "values"
            val valueTopicOne = "values_one"
            val valueTopicTwo = "values_two"
            val resultTopic = "output"

            val streamsBuilder = StreamsBuilder()

            val valuesJsonStream: KStream<String, String> = streamsBuilder
                .stream<String, String>(valueTopic, Consumed.with(Serdes.String(), Serdes.String()))

            val valuesTwoJsonStream: KStream<String, String> = streamsBuilder
                .stream<String, String>(valueTopicOne, Consumed.with(Serdes.String(), Serdes.String()))

            val valuesThreeJsonStream: KStream<String, String> = streamsBuilder
                .stream<String, String>(valueTopicTwo, Consumed.with(Serdes.String(), Serdes.String()))

            val allStream: KStream<String, String> = valuesJsonStream
                .merge(valuesTwoJsonStream)

            val mergedStream = allStream.merge(valuesThreeJsonStream)

            val resStream: KStream<String, String> = mergedStream.map { _, p ->
                KeyValue("data", p + "_output")
            }

            resStream.to(resultTopic, Produced.with(Serdes.String(), Serdes.String()))
            return streamsBuilder.build()
        }
    }
}