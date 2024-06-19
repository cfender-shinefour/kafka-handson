package de.shinefour.kafkastreams;

import de.shinefour.kafka.config.PropertiesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

public class MapExample {

    public static void main(String[] args) {
        var topology = buildTopology();
        var streams = new KafkaStreams(topology, PropertiesFactory.createProperties("app-map-example"));
        streams.start();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        builder.stream("daily-distance", Consumed.with(Serdes.String(), Serdes.Integer()).withName("BatteryLevelDistanceSource"))
                .mapValues(value -> new DistanceDTO(value * 0.001d), Named.as("MapToKilometers"))
                .to("daily-distance-km", Produced.with(Serdes.String(), new JsonSerde<>(DistanceDTO.class)).withName("DistanceKmOutput"));

        return builder.build();
    }

    public record DistanceDTO(double distance) { }

}
