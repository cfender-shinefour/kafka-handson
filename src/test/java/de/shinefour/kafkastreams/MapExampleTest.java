package de.shinefour.kafkastreams;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.assertj.core.api.Assertions.assertThat;

class MapExampleTest {

    private static TestInputTopic<String, Integer> inputTopic;
    private static TestOutputTopic<String, MapExample.DistanceDTO> outputTopic;
    private static final Topology topology = MapExample.buildTopology();

    @BeforeAll
    static void setup() {
        var testDriver = new TopologyTestDriver(topology);
        inputTopic = testDriver.createInputTopic("daily-distance", new StringSerializer(), new IntegerSerializer());

        var jsonSerde = new JsonSerde<>(MapExample.DistanceDTO.class);
        outputTopic = testDriver.createOutputTopic("daily-distance-km", new StringDeserializer(), jsonSerde.deserializer());
    }

    @Test
    void print() {
        System.out.println("topology = " + topology.describe());
    }

    @Test
    void shouldConvertMetersToKm() {
        inputTopic.pipeInput("iMow1", 1000);

        var result = outputTopic.readKeyValue();

        assertThat(result.key).isEqualTo("iMow1");
        assertThat(result.value.distance()).isEqualTo(1);
    }

}