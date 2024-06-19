package de.shinefour.kafkastreams;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DistanceAggregationTest {

    private static final String SAMPLE_DEVICE_ID = "iMow1";
    private static TestInputTopic<String, Integer> inputTopic;
    private static TestOutputTopic<String, Integer> outputTopic;
    private static Topology topology = DistanceAggregation.buildTopology();

    @BeforeAll
    static void setup() {
        var testDriver = new TopologyTestDriver(topology);
        inputTopic = testDriver.createInputTopic("daily-distance", new StringSerializer(), new IntegerSerializer());
        outputTopic = testDriver.createOutputTopic("total-distance", new StringDeserializer(), new IntegerDeserializer());
    }

    @Test
    void print() {
        System.out.println("topology = " + topology.describe());
    }

    @Test
    void shouldAggregateSum() {
        inputTopic.pipeInput(SAMPLE_DEVICE_ID, 10);
        inputTopic.pipeInput(SAMPLE_DEVICE_ID, 70);
        inputTopic.pipeInput(SAMPLE_DEVICE_ID, 20);

        var result = outputTopic.readKeyValuesToList();

        assertThat(result.getFirst()).isEqualTo(new KeyValue<>(SAMPLE_DEVICE_ID, 10));
        assertThat(result.get(1)).isEqualTo(new KeyValue<>(SAMPLE_DEVICE_ID, 80));
        assertThat(result.get(2)).isEqualTo(new KeyValue<>(SAMPLE_DEVICE_ID, 100));
    }

}