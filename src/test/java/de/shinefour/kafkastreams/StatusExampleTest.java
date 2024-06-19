package de.shinefour.kafkastreams;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.assertj.core.api.Assertions.assertThat;

class StatusExampleTest {

    private static TestInputTopic<String, Integer> batteryLevelTopic;
    private static TestInputTopic<String, Integer> totalDistanceTopic;
    private static TestOutputTopic<String, StatusExample.Status> outputTopic;
    private static final Topology topology = StatusExample.buildTopology();

    @BeforeAll
    static void setup() {
        var testDriver = new TopologyTestDriver(topology);
        batteryLevelTopic = testDriver.createInputTopic("status-battery-level", new StringSerializer(), new IntegerSerializer());
        totalDistanceTopic = testDriver.createInputTopic("total-distance", new StringSerializer(), new IntegerSerializer());

        var jsonSerde = new JsonSerde<>(StatusExample.Status.class);
        outputTopic = testDriver.createOutputTopic("status", new StringDeserializer(), jsonSerde.deserializer());
    }

    @Test
    void print() {
        System.out.println("topology = " + topology.describe());
    }

    @Test
    void shouldJoinBothTopics() {
        batteryLevelTopic.pipeInput("iMow1", 10);
        totalDistanceTopic.pipeInput("iMow1", 2000);

        var result = outputTopic.readKeyValuesToList();

        assertThat(result).contains(new KeyValue<>("iMow1", new StatusExample.Status(10, 2000)));
    }

}