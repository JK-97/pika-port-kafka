# 使用文档

本工具伪装为 PikiwiDB 的 slave，执行全量（snapshot）+ 增量（binlog）同步，并将事件输出到 Kafka。
已在 PikiwiDB 3.5.6（sha: 510ae73081190fbd85ec37b40c067b2b1aa9e615）验证。

## 1. 前置条件

- 可访问的 PikiwiDB master（支持 PB 复制或 legacy trysync/rsync）
- Kafka 可用，topic 已创建（含 compacted offsets topic）
- 已编译得到 `pika_port`（见 `docs/build.md`）

## 2. Topic 规划与模式

两种模式可选（通过 `-M` 切换）：

- **dual**：`snapshot` + `binlog` 两个 topic
- **single**：单 topic 混合输出

建议：

- `snapshot` 可短期保留；`binlog` 按回溯窗口配置
- `__pika_port_kafka_offsets` 设为 compacted，分区数 1

事件格式详见：`docs/pika-port-kafka-design.md`

## 3. 运行参数说明

必填/关键参数：

- `-i` master IP（默认值见配置）
- `-o` master 端口（必填）
- `-k` Kafka brokers（必填）
- `-c` Kafka client id（默认 `pika-port-kafka`）
- `-M` stream 模式：`dual` 或 `single`（默认 `dual`）
- `-S` snapshot topic（`dual` 必填）
- `-B` binlog topic（`dual` 必填）
- `-T` single topic（`single` 必填）
- `-R` 同步协议：`auto` | `legacy` | `pb`（默认 `auto`）

断点恢复与标识：

- `-P` checkpoint 文件路径（默认 `./checkpoint.json`）
- `-O` offsets topic（默认 `__pika_port_kafka_offsets`）
- `-U` source_id（默认 `master_ip:master_port`）
- `-f/-s` 手动指定 binlog filenum/offset（设置后不会加载 checkpoint）

网络/端口（legacy 模式）：

- `-t` 本机可被 master 访问的 IP（默认值见配置）
- `-p` 本机端口（默认值见配置）
  - binlog 接收端口：`local_port + <binlog_port_offset>`（偏移在代码中固定）
  - rsync 端口：`local_port + <rsync_port_offset>`（偏移在代码中固定）
  - 以上端口需被 master 访问

网络/端口（pb/auto 模式）：

- 本地无需对外开放端口
- 会访问 master 的 `master_port + 2000`（PB 复制）与 `master_port + 10001`（rsync2）

性能/运行：

- `-x` Kafka 发送线程数（默认 `1`）
- `-b` snapshot 扫描批量（默认 `512`）
- `-z` 等待 master bgsave 超时（默认 `1800` 秒）
- `-E` enable idempotence（默认 `true`）
- `-w` master 密码（可选）
- `-l` log 路径（默认 `./log/`）
- `-r` dump 路径（默认 `./rsync_dump/`）
- `-D` db_name label（默认 `db0`）
- `-d` daemon 模式（可选）
- `-e` 若遇到 dbsync wait 立即退出（可选）
- `-H` 心跳日志间隔秒数（默认 `60`，最小 `1`，`0` 关闭）
- `-J` Kafka producer `message.max.bytes`（默认 `1000000`）

说明：

- `-R auto` 会优先探测 PB 复制端口，失败则回退 legacy。

## 4. 示例

### 4.1 dual 模式（snapshot + binlog）

```bash
./build/pika_port \
  -t <LOCAL_IP> -p <LOCAL_PORT> \
  -i <PIKA_MASTER_IP> -o <PIKA_MASTER_PORT> \
  -k <KAFKA_BROKERS> \
  -M dual -S <SNAPSHOT_TOPIC> -B <BINLOG_TOPIC> -O <OFFSETS_TOPIC> -R auto \
  -P ./checkpoint.json -D db0 -x 4 -b 512 -z 1800 -E true \
  -l ./log -r ./rsync_dump
```

### 4.2 single 模式（单流顺序消费）

```bash
./build/pika_port \
  -t <LOCAL_IP> -p <LOCAL_PORT> \
  -i <PIKA_MASTER_IP> -o <PIKA_MASTER_PORT> \
  -k <KAFKA_BROKERS> \
  -M single -T <STREAM_TOPIC> -O <OFFSETS_TOPIC> -R auto \
  -P ./checkpoint.json -D db0 -x 4 -b 512 -z 1800 -E true \
  -l ./log -r ./rsync_dump
```

## 5. 断点恢复与重跑

- 默认会读取 `checkpoint.json` 与 offsets topic 中的最新位置。
- 若要从头全量：删除 checkpoint 文件，并清理 offsets topic 中对应 `source_id` 的记录。
- 若要指定位置：使用 `-f <filenum> -s <offset>`。

## 6. 运行状态判断

日志默认输出到 `./log/`，启动后应看到：

- `Trysync success`
- `Finish to start rsync`
- Kafka 发送统计信息

如 master 无法访问本机 rsync / binlog 端口，先检查 `-t` 是否为可达 IP，以及防火墙规则。
