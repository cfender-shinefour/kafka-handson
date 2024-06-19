# Kafka hands-on
## Spin up local kafka
```
docker compose up -d
```

## Kafka CLI

Download: https://kafka.apache.org/downloads

### Display consumer group details
```
./kafka-consumer-groups.sh --bootstrap-server localhost:29092 --group my-group-id --describe
```

### Reset offset of consumer
```
./kafka-consumer-groups.sh --bootstrap-server localhost:29092 --group my-group-id --topic my-topic --reset-offsets --to-earliest --execute
```

## Kafka Streams DSL
https://kafka.apache.org/37/documentation/streams/developer-guide/dsl-api.html

## Kafka Streams Visualizer
https://zz85.github.io/kafka-streams-viz/
