package de.shinefour.kafkastreams;

import de.shinefour.kafka.config.PropertiesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class DistanceAggregation {

    public static void main(String[] args) {
        var topology = buildTopology();
        var streams = new KafkaStreams(topology, PropertiesFactory.createProperties("distance-aggregation"));
        streams.start();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        builder.stream("daily-distance", Consumed.with(Serdes.String(), Serdes.Integer()).withName("DailyDistanceSource"));
        // Aufgabe: Ein IMow sendet täglich die gefahrene Distanz auf dem Topic "daily-distance". Die Nachrichten haben 
        // folgendes Format:
        // <key: "iMow1", value: 10>
        // <key: "iMow1", value: 70>
        // <key: "iMow1", value: 20>
        //
        // Die gefahrene Distanz soll aufsummiert und im Topic "total-distance" ausgegeben werden. Als Ausgabe wird dabei
        // folgendes erwartet:
        // <key: "iMow1", value: 10>
        // <key: "iMow1", value: 80>
        // <key: "iMow1", value: 100>

        return builder.build();
    }

}
