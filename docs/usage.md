# 使用文档

本工具伪装为 PikiwiDB 的 slave，执行全量（snapshot）+ 增量（binlog）同步，并将事件输出到 Kafka。
已在 PikiwiDB 3.5.6（sha: 510ae73081190fbd85ec37b40c067b2b1aa9e615）验证。

## 1. 前置条件

- 可访问的 PikiwiDB master（支持 PB 复制或 legacy trysync/rsync）
- Kafka 可用，topic 已创建（含 compacted offsets topic）
- 已编译得到 `pika_port`（见 `docs/build.md`）
- 配置文件示例见 `docs/pika-port-kafka.conf.example`

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

- `-C / --config` 配置文件路径（`key=value`，支持 `#`/`;` 注释，配置 < CLI 覆盖）
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
- `-Q` 是否写 offsets topic（`true|false`，默认 `true`）
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
- `--binlog_workers` PB binlog 解析/过滤/构建 worker 数（默认 `4`）
- `--binlog_queue_size` PB binlog worker 队列大小（默认 `4096`）
- `-G` Heartbeat 中 Kafka sender 统计输出模式（`none|agg|detail|all`，默认 `agg`）
  - `agg` 输出聚合视图，`detail` 输出每个 sender 明细，`all` 同时输出
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
- `--snapshot_oversize_list_tail_max_items` list snapshot 超限时仅发送末尾 N 条（默认 `0`，关闭）
- `--snapshot_oversize_shrink_batch` hash/set/zset snapshot 超限时自动缩小批次（默认 `true`）
- `--snapshot_oversize_string_policy` string snapshot 超限策略：`skip|error`（默认 `skip`）
- `--args_encoding` 事件 `args` 字段编码：`base64|none`（默认 `base64`）
- `--raw_resp_encoding` 事件 `raw_resp` 字段编码：`base64|none`（默认 `base64`）
- `--include_raw_resp` 是否输出 `raw_resp` 字段（`true|false`，默认 `true`）
- `-A` PB ack 延迟告警秒数（默认 `10`，最小 `1`，`0` 关闭）
- `-I` PB 空闲自动重连秒数（默认 `30`，最小 `1`，`0` 关闭）
- `-F` 事件过滤规则组（可重复）
- `-X` 全局 exclude key 规则（逗号分隔）
- `--start_from_master` 启动时读取 master `INFO replication` 的 `binlog_offset` 覆盖本地 checkpoint（`true|false`，默认 `false`）

说明：

- `-R auto` 会优先探测 PB 复制端口，失败则回退 legacy。
- `--snapshot_oversize_*` 选项仅对 snapshot 生效，不影响 binlog。
- `args/raw_resp` 选择 `none` 编码时将输出原始字符串，可能包含非 UTF-8/二进制数据。
- `start_from_master=true` 会忽略本地 checkpoint，从 master 当前 `binlog_offset` 开始同步。

## 4. 事件过滤

过滤仅影响是否发送到 Kafka，不阻塞同步进度（checkpoint 仍推进）。默认不配置时全量发送。

规则语义：

- 组内：`key` + `type` + `action` 为 AND 关系
- 组间：OR 关系（命中任一组即通过）
- 全局 exclude：优先级最高，命中即丢弃

配置方式：

- `-F "key=dev:*;type=list;action=lpush"`
- `-F "key=prod:*;type=list,set;action=rpush"`
- `-F /path/to/filters.conf`（文件中一行一个规则组，支持空行和 `#` 注释）
- `-X "tmp:*,bad:*"`（全局 exclude）
- 配置文件：
  - `filter=key=dev:*;type=list;action=rpush`
  - `exclude=tmp:*,bad:*`
  - `exclude_keys=secret:*`

key 规则：

- 以 `*` 结尾且不含其它正则元字符时视为前缀（如 `dev:*`）
- 包含正则元字符（如 `.` `+` `[]` `()` `|` `^` `$` 等）时按正则处理
- 既无 `*` 也无正则元字符时为精确匹配

type/action 来源：

- `type`：`string|hash|list|set|zset|unknown`
- `action`：Redis 命令名（大小写不敏感，例如 `lpush`、`rpush`、`expire`）
- 若 type/action 无法识别，则默认放行（不因该条件过滤）

## 5. 示例

### 5.1 dual 模式（snapshot + binlog）

```bash
./build/pika_port \
  -t <LOCAL_IP> -p <LOCAL_PORT> \
  -i <PIKA_MASTER_IP> -o <PIKA_MASTER_PORT> \
  -k <KAFKA_BROKERS> \
  -M dual -S <SNAPSHOT_TOPIC> -B <BINLOG_TOPIC> -O <OFFSETS_TOPIC> -R auto \
  -P ./checkpoint.json -D db0 -x 4 -b 512 -z 1800 -E true \
  -l ./log -r ./rsync_dump
```

### 5.2 single 模式（单流顺序消费）

```bash
./build/pika_port \
  -t <LOCAL_IP> -p <LOCAL_PORT> \
  -i <PIKA_MASTER_IP> -o <PIKA_MASTER_PORT> \
  -k <KAFKA_BROKERS> \
  -M single -T <STREAM_TOPIC> -O <OFFSETS_TOPIC> -R auto \
  -P ./checkpoint.json -D db0 -x 4 -b 512 -z 1800 -E true \
  -l ./log -r ./rsync_dump
```

## 6. 断点恢复与重跑

- 默认会读取 `checkpoint.json` 与 offsets topic 中的最新位置。
- 若要从头全量：删除 checkpoint 文件，并清理 offsets topic 中对应 `source_id` 的记录。
- 若要指定位置：使用 `-f <filenum> -s <offset>`。

## 7. 运行状态判断

日志默认输出到 `./log/`，启动后应看到：

- `Trysync success`
- `Finish to start rsync`
- Kafka 发送统计信息
- Heartbeat 日志包含 `master_lag`（来自 master `INFO replication`）

如 master 无法访问本机 rsync / binlog 端口，先检查 `-t` 是否为可达 IP，以及防火墙规则。
