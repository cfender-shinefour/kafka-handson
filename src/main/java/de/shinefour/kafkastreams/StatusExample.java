package de.shinefour.kafkastreams;

import de.shinefour.kafka.config.PropertiesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;

public class StatusExample {

    public static void main(String[] args) {
        var topology = buildTopology();
        var streams = new KafkaStreams(topology, PropertiesFactory.createProperties("app-status-example"));
        streams.start();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        var batteryLevel = builder.stream(
                "status-battery-level",
                Consumed.with(Serdes.String(), Serdes.Integer()).withName("BatteryLevelDistanceSource")
        ).toTable(createMaterialized("BatteryLevelDistanceStore"));

        var totalDistance = builder.stream(
                "total-distance",
                Consumed.with(Serdes.String(), Serdes.Integer()).withName("TotalDistanceSource")
        ).toTable(createMaterialized("TotalDistanceStore"));

        // Aufgabe: Die Werte aus beiden Topics sollen zu einem Status zusammengefasst werden.
        // Eingangstopic: status-battery-level:  <key: "iMow1", value: 10>
        // Eingangstopic: total-distance:        <key: "iMow1", value: 2000>
        // Ausgabe:       status                 <key: "iMow1", value: {"batteryLevel": 10, "totalDistance": 2000}

        return builder.build();
    }

    private static Materialized<String, Integer, KeyValueStore<Bytes, byte[]>> createMaterialized(String storeName) {
        return Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Integer())
                .withCachingDisabled();
    }

    public record Status(Integer batteryLevel, Integer totalDistance) { }


}
