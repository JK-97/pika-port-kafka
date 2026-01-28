# FAQ

本页记录 pika-port-kafka 在部署与运行中常见问题及处理建议。

## 1) Kafka 报 “Failed to resolve 'kafka:9092'”

- 现象：`Failed to resolve 'kafka:9092'` 或 `Name or service not known`
- 原因：Kafka `advertised.listeners` 使用了容器内主机名，客户端无法解析
- 处理：
  - 将 `advertised.listeners` 改为客户端可访问的地址（宿主机 IP/域名）
  - 重新启动 Kafka

## 2) Kafka 报 “topic does not exist”

- 现象：`__pika_port_kafka_offsets [0]: topic does not exist`
- 原因：禁用 `auto.create.topics.enable` 时不会自动创建
- 处理：手动创建 topic（其中 offsets topic 建议 `cleanup.policy=compact`）

## 3) Pika 返回 “ERR unknown command 'trysync'”

- 现象：`Reply from master after trysync: ERR unknown command "trysync"`
- 原因：使用 legacy 协议连接到仅支持 PB 协议的 Pika
- 处理：启动时指定 `-R pb` 或 `-R auto`

## 4) PB 连接失败：`EHOSTUNREACH`

- 现象：`pb repl: connect failed IO error: EHOSTUNREACH`
- 原因：网络不可达或复制相关端口未开放/未映射
- 处理：
  - 检查容器/防火墙开放端口
  - 确保复制端口可从客户端访问

## 5) PB 解析错误：`missing required fields db_instance_num`

- 现象：`Can't parse message ... missing required fields: meta_sync.dbs_info[0].db_instance_num`
- 原因：PB 协议版本不匹配
- 处理：
  - 确认 pikiwidb 与 pika-port-kafka 使用兼容版本
  - 升级到最新构建后重试

## 6) PB TrySync error code 3

- 现象：`pb repl: TrySync error code 3`
- 原因：checkpoint 记录的 binlog 位点超出主库范围
- 处理：
  - 删除本地 `checkpoint.json` 后重启
  - 或启动时指定 `-f 0 -s 0` 强制从头同步

## 7) rsync/rsync2 失败

- 现象：`Failed to start rsync` 或 `connecting to rsync failed`
- 原因：dump 目录不存在/无权限，或 rsync 端口未开放
- 处理：
  - 确保 `rsync_dump` 目录存在且可写
  - 检查容器端口映射及防火墙

## 8) 指定 `-f 0 -s 0` 后出现大量 `connection accepted/closed`

- 现象：启动后 rsync/rsync2 日志里出现大量 `connection accepted/closed`
- 原因：`-f 0 -s 0` 强制从头同步，会触发全量 DBSync/rsync（短连接是正常行为）
- 处理：
  - 如果不是刻意全量同步，去掉 `-f 0 -s 0`，改用 `checkpoint.json` 或 offsets topic 续传
  - 日志会提示：`Start offset forced to 0: full sync (DBSync/rsync) may be triggered`

## 9) rsync2: failed to create path / MANIFEST 不存在

- 现象：`failed to create path ... MANIFEST-xxxxx`
- 原因：目录未创建/权限不足/残留文件导致冲突
- 处理：
  - 清空并重新创建 `rsync_dump`
  - 确保路径可写

## 10) Kafka 报 “Broker: Invalid message”

- 现象：`Kafka delivery failed: Broker: Invalid message`
- 原因：消息体超过 Kafka 限制或配置不一致
- 处理：
  - 调大 broker 与 topic 的消息大小限制
  - 同步调整生产端/消费端配置的 `message.max.bytes` 等参数

## 11) “Coordinator load in progress”

- 现象：`Failed to acquire idempotence PID ... Coordinator load in progress`
- 原因：Kafka 协调器在初始化，属于短暂现象
- 处理：
  - 等待重试即可
  - 如需快速验证，可临时关闭 `-E` 幂等发送

## 12) 复制端口与 rsync2 端口说明

- 现象：同步会连接 `master_port+2000` 与 `master_port+10001`
- 说明：
  - `master_port+2000` 通常用于复制/增量 binlog 流
  - `master_port+10001` 通常用于全量数据传输（rsync/rsync2）
  - legacy 协议使用 trysync + rsync，PB 协议使用 MetaSync/TrySync + rsync2
