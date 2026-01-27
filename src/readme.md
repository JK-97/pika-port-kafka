## pika_port
---
  **written by AlexStocks(github.com/AlexStocks) 2018/09**

## Intro

This fork converts pika-port into a Kafka CDC producer. It copies a snapshot from Pika and then streams binlog changes
to Kafka topics (snapshot + binlog, or single stream mode).

If u wanna get more details of Pika-port, pls read [Pika笔记](http://alexstocks.github.io/html/pika.html).

This repo targets Pika v3.0.x.

## Build Notes

- Requires `librdkafka` development headers and library.

## Use Case

* [记一次pika迁移到codis](https://blog.csdn.net/wangwenjie2500/article/details/83858572)

## Version list

> V1.6
 * Improvement: add wait-bgsave-timeout to give a warning when pika-port waits bgsave data for a long time
	* Improvement: change rsync configure file's module name from document_${master_ip}:${master_port} to document_${slave_ip}:master_port

> V1.5
	* Improvement: add batch parameter to speed up transfering data between pika and pika/codis

> V1.4
	* Bug Fix: filter out SlotKeyPrefix when sync snapshot data

> V1.7
	* Kafka CDC output, JSON payload, checkpoint (local + compacted topic)
	* Stream mode: dual topics or single stream

> V1.1
	* filter out SlotKeyPrefix
	* disable ping-pong log

> V1.0
	* Init
