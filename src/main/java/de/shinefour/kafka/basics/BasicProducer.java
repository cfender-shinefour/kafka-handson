package de.shinefour.kafka.basics;

import de.shinefour.Environment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class BasicProducer {

    private static final Logger LOG = LoggerFactory.getLogger(BasicProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Environment.BOOTSTRAP_SERVER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        createTopic(props, "my-topic", 3);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            IntStream.range(0, 100).forEach(i -> {
                        producer.send(new ProducerRecord<>("my-topic", "Message " + i, "Key " + i), (metadata, exception) -> {
                            if (exception != null) {
                                LOG.error("Trouble producing", exception);
                            } else {
                                LOG.debug("Produced record at offset {} to topic {}", metadata.offset(), metadata.topic());
                            }
                        });
                        sleep(1000);
                    }
            );
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void createTopic(Properties props, String topicName, int partitions) {
        try (var adminClient = AdminClient.create(props)) {
            adminClient.createTopics(List.of(new NewTopic(topicName, partitions, (short) 1))).all().get();
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // Nothing to do here
                return;
            }
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
