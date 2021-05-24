
import com.kafka.demo.controller.CustomKafkaStream
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test
import java.util.Arrays
import java.util.Locale
import java.util.Properties

class TopologyTest {

    private val recordFactory = ConsumerRecordFactory(Serdes.String().serializer(), Serdes.String().serializer())
    private val props = properties(APPLICATION_ID, "localhost:9092")

    @Test
    fun test() {

        val driver = TopologyTestDriver(
            CustomKafkaStream.build(), props
        )

        val inputValues = Arrays.asList("Vastuullisuus", "Asiakaslähtöisyys", "Yhteisöllisyys")
        val expectedList = Arrays.asList(
            ProducerRecord(OUTPUT_TOPIC, "data", "Vastuullisuus_output"),
            ProducerRecord(OUTPUT_TOPIC, "data", "Asiakaslähtöisyys_output"),
            ProducerRecord(OUTPUT_TOPIC, "data", "Yhteisöllisyys_output")
        )

        // Feed Input
        for (inputValue in inputValues) {
            driver.pipeInput(recordFactory.create(INPUT_TOPIC, null, inputValue))
        }

        // Retrieve Output
        for (expected in expectedList) {
            val actual = driver.readOutput(OUTPUT_TOPIC, Serdes.String().deserializer(),Serdes.String().deserializer() )
            assertThat(actual.key(), equalTo(expected.key()))
            assertThat(actual.value(), equalTo(expected.value()))
        }
    }

    fun properties(appId: String, bootstrap: String): Properties {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = appId
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
        props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return props
    }


    companion object {
        private val APPLICATION_ID = "test-app-id"
        private val INPUT_TOPIC = "values"
        private val OUTPUT_TOPIC = "output"
    }
}