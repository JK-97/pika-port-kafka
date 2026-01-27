# pika-port-kafka

Kafka CDC fork of pika-port. It consumes Pika snapshot + binlog and publishes JSON events to Kafka, with checkpoint
recovery (local + compacted topic).

Design doc: `docs/pika-port-kafka-design.md`
Build doc: `docs/build.md`
Usage doc: `docs/usage.md`

Requires `librdkafka` for build/runtime.
