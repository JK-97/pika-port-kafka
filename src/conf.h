#ifndef BINLOG_CONF_H_
#define BINLOG_CONF_H_

#include <memory>
#include <string>

class Conf {
 public:
  Conf() {
    local_ip = "127.0.0.1";
    local_port = 0;
    master_ip = "127.0.0.1";
    master_port = 0;
    forward_ip = "127.0.0.1";
    forward_port = 0;
    forward_thread_num = 1;
    filenum = size_t(UINT32_MAX);  // src/pika_trysync_thread.cc:48
    offset = 0;
    log_path = "./log/";
    dump_path = "./rsync_dump/";
    sync_batch_num = 512;
    wait_bgsave_timeout = 1800;  // 30 minutes
    exit_if_dbsync = false;
    kafka_brokers = "";
    kafka_client_id = "pika-port-kafka";
    kafka_topic_snapshot = "pika.snapshot";
    kafka_topic_binlog = "pika.binlog";
    kafka_topic_single = "pika.stream";
    kafka_topic_offsets = "__pika_port_kafka_offsets";
    checkpoint_path = "./checkpoint.json";
    source_id = "";
    db_name = "db0";
    kafka_enable_idempotence = true;
    kafka_stream_mode = "dual";
    sync_protocol = "auto";
    heartbeat_interval_ms = 60000;
  }

 public:
  size_t filenum;
  size_t offset;
  std::string local_ip;
  int local_port;
  std::string master_ip;
  int master_port;
  std::string forward_ip;
  int forward_port;
  std::string forward_passwd;
  int forward_thread_num;
  std::string passwd;
  std::string log_path;
  std::string dump_path;
  size_t sync_batch_num;
  time_t wait_bgsave_timeout;
  bool exit_if_dbsync;

  std::string kafka_brokers;
  std::string kafka_client_id;
  std::string kafka_topic_snapshot;
  std::string kafka_topic_binlog;
  std::string kafka_topic_single;
  std::string kafka_topic_offsets;
  std::string checkpoint_path;
  std::string source_id;
  std::string db_name;
  bool kafka_enable_idempotence;
  std::string kafka_stream_mode;
  std::string sync_protocol;
  int64_t heartbeat_interval_ms;
};

extern Conf g_conf;

#endif
