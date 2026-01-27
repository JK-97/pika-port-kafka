# Pika-Port Kafka 改造方案（方案 B）

> 目标：在不修改 PikiwiDB 的前提下，将 pika-port 改造为 Kafka CDC 进程，输出全量与增量事件到 Kafka，并提供可靠的断点恢复与幂等保证。

## 1. 范围与原则

- 进程形态：独立进程，伪装 Pika slave 与 master 建立同步。
- 同步链路：兼容 legacy trysync/dbsync/binlog 与 PB 复制（MetaSync/TrySync/DBSync/BinlogSync）。
- 协议选择：`sync_protocol=auto|legacy|pb`（auto 优先 PB，失败回退 legacy）。
- 交付目标：支持全量同步、增量同步、断点恢复、幂等消费。
- 最小改动：复用 pika-port 现有解析与过滤逻辑，聚焦在数据输出层与状态管理层。

## 2. Topic 规划（双流 + 单流可配）

支持两种输出方式，通过配置选择：

- **双流模式**：`snapshot` 与 `binlog` 分开 topic
- **单流模式**：全部事件写入同一个 topic（便于顺序消费）

双流模式：

- `pika.<cluster>.snapshot`
  - 全量同步阶段输出的数据
  - 便于独立保留与回放
- `pika.<cluster>.binlog`
  - 增量同步阶段输出的数据

单流模式：

- `pika.<cluster>.stream`
  - 全量与增量事件混合写入
  - 消费端根据 `event_type` 处理
  - 若需要严格顺序消费，建议设置单分区
- `__pika_port_kafka_offsets`（Compacted）
  - 存储 checkpoint，用于断点恢复
  - 建议 1 个分区，便于按 key 快速加载最新 checkpoint

建议命名示例（cluster=prod）：

- 双流：
- `pika.prod.snapshot`
- `pika.prod.binlog`
- 单流：
- `pika.prod.stream`
- `__pika_port_kafka_offsets`

### 2.1 Partition Key

- 默认：`db + ":" + data_type + ":" + key`
- 无 key 的命令（如 flushdb/flushall）：`db` 作为 key

这样可以保证“同 key 同分区顺序一致”，同时可扩展多分区吞吐。

### 2.2 模式切换配置（建议）

- `kafka_stream_mode=dual|single`
  - `dual`：使用 `snapshot` + `binlog` 两个 topic
  - `single`：使用 `stream` 单 topic
- `kafka_topic_snapshot`
- `kafka_topic_binlog`
- `kafka_topic_single`

### 2.3 保留策略建议

- `snapshot` 可短保留（例如 1~7 天，取决于是否需要反复回放）
- `binlog` 保留 >= 最大回溯窗口（建议 >= 最大停机/修复时间）
- `offsets` compacted 永久保留

## 3. Kafka 消息格式（JSON）

### 3.1 通用结构

```json
{
  "event_type": "snapshot | binlog",
  "op": "set | hset | sadd | del | ...",
  "data_type": "string | hash | list | zset | set",
  "db": "db0",
  "slot": 0,
  "key": "user:1",
  "args_b64": ["<base64>", "<base64>"],
  "raw_resp_b64": "<base64 RESP command>",
  "ts_ms": 1700000000,
  "event_id": "1:12:34567:8899",
  "source_id": "10.0.0.1:9221",
  "binlog": {
    "filenum": 12,
    "offset": 34567,
    "logic_id": 8899,
    "server_id": 1,
    "term_id": 0
  },
  "source": {
    "host": "10.0.0.1",
    "port": 9221,
    "replication_id": "abc"
  }
}
```

### 3.2 说明

- `event_type`
  - `snapshot`：来自全量同步（dbsync）
  - `binlog`：来自增量同步（binlog）
- `args_b64` 与 `raw_resp_b64`
  - 避免二进制内容导致 JSON 非法
  - 消费端可选择是否还原为原始命令
- `event_id`
  - legacy：`server_id:filenum:offset:logic_id`
  - pb：`term_id:filenum:offset:logic_id`
- `source_id`
  - 例如 `host:port`，用于 checkpoint 与 offsets topic 的 key
- `binlog` 字段
  - `filenum + offset` 作为“主偏移”
  - `logic_id` 作为附加校验
  - `server_id` 与 `term_id` 按协议二选一

## 4. Offset 设计建议

- 逻辑偏移：`filenum + offset`（主）
- 校验偏移：`logic_id`（次）
- 绝对偏移（可选）：`abs_offset = filenum * binlog_file_size + offset`

## 5. Checkpoint 设计（本地 + Kafka Compacted）

### 5.1 本地文件

- 路径：`/var/lib/pika-port-kafka/checkpoint.json`
- 内容示例：

```json
{
  "source_id": "10.0.0.1:9221",
  "filenum": 12,
  "offset": 34567,
  "logic_id": 8899,
  "server_id": 1,
  "term_id": 0,
  "ts_ms": 1700000000
}
```

### 5.2 Kafka Compacted Topic

- Topic：`__pika_port_kafka_offsets`
- Key：`source_id`（例如 `host:port`）
- Value：与本地一致

### 5.3 更新策略

- Kafka 生产成功（收到 ack）后更新 checkpoint
- 先写本地，再写 compacted topic（或启用事务保证一致性）

## 6. 断点恢复流程

1. 启动时读取本地 checkpoint（若不存在则读 compacted topic）
2. 有 checkpoint → `trysync filenum offset`（legacy）或 `TrySync`（pb）
3. 无 checkpoint → `trysync 0 0` / `TrySync 0 0` 触发全量同步
4. 如果 master 返回 `sync point purged` → 自动回退到全量同步

## 7. 幂等与去重路径

### 7.1 事件唯一键

- `event_id = server_id:filenum:offset:logic_id`
- 用于消费端去重或幂等写入

### 7.2 生产端配置（Kafka）

- `enable.idempotence=true`
- `acks=all`
- `max.in.flight.requests.per.connection=1`

### 7.3 消费端保证

- 以 `event_id` 做幂等写入
- 同 key 顺序消费

## 8. 可选本地 WAL（增强可靠性）

为防止 Kafka 不可用导致数据丢失：

- 接收到 binlog 后先写本地 append-only log
- Kafka ack 后标记为已消费
- 重启时重放未 ack 的本地 WAL

## 9. 同步流程（改造后）

1. 进程启动 → 读取 checkpoint
2. 建立 legacy trysync 或 PB MetaSync/TrySync → 全量或增量
3. 全量阶段：dump → 解析 → `snapshot topic`
4. 增量阶段：binlog → 解析 → `binlog topic`
5. Kafka ack → 更新 checkpoint（本地 + compacted）

## 10. 配置建议（新增）

- Kafka
  - brokers
  - topic_snapshot / topic_binlog / topic_offsets
  - client_id
  - enable_idempotence
- Checkpoint
  - checkpoint_path
  - source_id
- 全量/增量
  - forward_thread_num（可复用现有）
  - filter_rules（命令/DB/数据结构过滤）

## 11. 监控与告警指标（建议）

- 当前同步点（filenum/offset）
- binlog lag（bytes 或 offset 差）
- Kafka 发送失败率/重试次数
- snapshot/binlog 处理 QPS
- checkpoint 更新时间

## 12. 风险与边界

- 同 key 不同数据结构：必须依赖 `data_type` 字段区分
- 跨 key 的全局顺序无法保证（Kafka 分区模型限制）
- `snapshot` 与 `binlog` 间可能存在时间重叠，需要消费端按 `event_type` 与 `binlog offset` 做归并

## 13. 后续演进

- JSON → Protobuf/Avro
- Kafka 事务：事件 + checkpoint 原子提交
- 统一 Schema Registry 与版本演进策略
