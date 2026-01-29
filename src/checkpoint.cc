#include "checkpoint.h"

#include "pika_binlog.h"

#include <chrono>
#include <fstream>
#include <sstream>

#include <glog/logging.h>

namespace {

const uint64_t kFilteredCheckpointBatch = 256;
const auto kFilteredCheckpointDelay = std::chrono::milliseconds(1000);

}  // namespace

CheckpointManager::CheckpointManager(std::string path,
                                     std::string source_id,
                                     std::string offsets_topic,
                                     std::string brokers)
    : path_(std::move(path)),
      source_id_(std::move(source_id)),
      offsets_topic_(std::move(offsets_topic)),
      brokers_(std::move(brokers)) {
  last_filtered_persist_ = std::chrono::steady_clock::now();
}

bool CheckpointManager::Load(Checkpoint* out) {
  if (LoadFromFile(out)) {
    return true;
  }
  if (!brokers_.empty() && !offsets_topic_.empty()) {
    return LoadFromKafka(out);
  }
  return false;
}

bool CheckpointManager::GetLast(Checkpoint* out) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!has_last_) {
    return false;
  }
  *out = last_;
  return true;
}

void CheckpointManager::SetBinlog(Binlog* binlog) {
  std::lock_guard<std::mutex> lock(mutex_);
  binlog_ = binlog;
}

bool CheckpointManager::LoadFromFile(Checkpoint* out) {
  std::ifstream in(path_);
  if (!in.is_open()) {
    return false;
  }
  std::stringstream buffer;
  buffer << in.rdbuf();
  in.close();
  Checkpoint cp;
  if (!ParseCheckpointJson(buffer.str(), &cp)) {
    return false;
  }
  *out = cp;
  return true;
}

bool CheckpointManager::LoadFromKafka(Checkpoint* out) {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();
  rd_kafka_conf_set(conf, "bootstrap.servers", brokers_.c_str(), errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "group.id", "pika-port-kafka-offsets-loader", errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));
  rd_kafka_conf_set(conf, "enable.partition.eof", "true", errstr, sizeof(errstr));

  rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!consumer) {
    LOG(WARNING) << "Offsets load: create consumer failed: " << errstr;
    return false;
  }
  rd_kafka_poll_set_consumer(consumer);

  rd_kafka_topic_partition_list_t* partitions = rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(partitions, offsets_topic_.c_str(), 0);
  rd_kafka_assign(consumer, partitions);
  rd_kafka_topic_partition_list_destroy(partitions);

  bool found = false;
  Checkpoint latest;

  while (true) {
    rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer, 1000);
    if (!msg) {
      continue;
    }
    if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      rd_kafka_message_destroy(msg);
      break;
    }
    if (msg->err) {
      LOG(WARNING) << "Offsets load: consume error: " << rd_kafka_message_errstr(msg);
      rd_kafka_message_destroy(msg);
      continue;
    }

    if (msg->key && source_id_ == std::string(static_cast<const char*>(msg->key), msg->key_len)) {
      std::string value(static_cast<const char*>(msg->payload), msg->len);
      Checkpoint cp;
      if (ParseCheckpointJson(value, &cp)) {
        latest = cp;
        found = true;
      }
    }
    rd_kafka_message_destroy(msg);
  }

  rd_kafka_consumer_close(consumer);
  rd_kafka_destroy(consumer);

  if (found) {
    *out = latest;
  }
  return found;
}

void CheckpointManager::OnAck(rd_kafka_t* producer, const Checkpoint& cp) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!IsNewer(cp)) {
    return;
  }
  last_ = cp;
  has_last_ = true;
  PersistLocal(cp);
  PersistKafka(producer, cp);
  last_filtered_persist_ = std::chrono::steady_clock::now();
  filtered_since_persist_ = 0;
  if (binlog_) {
    binlog_->SetProducerStatus(cp.filenum, cp.offset);
  }
}

void CheckpointManager::OnFiltered(const Checkpoint& cp) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!IsNewer(cp)) {
    return;
  }
  last_ = cp;
  has_last_ = true;
  if (binlog_) {
    binlog_->SetProducerStatus(cp.filenum, cp.offset);
  }

  filtered_since_persist_++;
  auto now = std::chrono::steady_clock::now();
  if (filtered_since_persist_ >= kFilteredCheckpointBatch ||
      now - last_filtered_persist_ >= kFilteredCheckpointDelay) {
    PersistLocal(cp);
    last_filtered_persist_ = now;
    filtered_since_persist_ = 0;
  }
}

void CheckpointManager::FlushFiltered() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!has_last_ || filtered_since_persist_ == 0) {
    return;
  }
  PersistLocal(last_);
  last_filtered_persist_ = std::chrono::steady_clock::now();
  filtered_since_persist_ = 0;
}

bool CheckpointManager::IsNewer(const Checkpoint& cp) const {
  if (!has_last_) {
    return true;
  }
  if (cp.filenum > last_.filenum) {
    return true;
  }
  if (cp.filenum == last_.filenum && cp.offset > last_.offset) {
    return true;
  }
  return false;
}

void CheckpointManager::PersistLocal(const Checkpoint& cp) {
  std::ofstream out(path_, std::ios::trunc);
  if (!out.is_open()) {
    LOG(WARNING) << "Failed to write checkpoint file: " << path_;
    return;
  }
  out << CheckpointToJson(cp);
  out.close();
}

void CheckpointManager::PersistKafka(rd_kafka_t* producer, const Checkpoint& cp) {
  if (!producer || offsets_topic_.empty()) {
    return;
  }
  std::string payload = CheckpointToJson(cp);
  rd_kafka_resp_err_t err = rd_kafka_producev(
      producer,
      RD_KAFKA_V_TOPIC(offsets_topic_.c_str()),
      RD_KAFKA_V_KEY(source_id_.data(), source_id_.size()),
      RD_KAFKA_V_VALUE(payload.data(), payload.size()),
      RD_KAFKA_V_END);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    LOG(WARNING) << "Failed to publish checkpoint to Kafka: " << rd_kafka_err2str(err);
  }
}

std::string CheckpointManager::CheckpointToJson(const Checkpoint& cp) const {
  std::ostringstream ss;
  ss << "{\"source_id\":\"" << source_id_ << "\"";
  ss << ",\"filenum\":" << cp.filenum;
  ss << ",\"offset\":" << cp.offset;
  ss << ",\"logic_id\":" << cp.logic_id;
  ss << ",\"server_id\":" << cp.server_id;
  ss << ",\"term_id\":" << cp.term_id;
  ss << ",\"ts_ms\":" << cp.ts_ms;
  ss << "}";
  return ss.str();
}

bool CheckpointManager::ParseCheckpointJson(const std::string& json, Checkpoint* out) const {
  auto find_number = [&](const std::string& key, uint64_t* value) -> bool {
    size_t pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) {
      return false;
    }
    pos = json.find(':', pos);
    if (pos == std::string::npos) {
      return false;
    }
    pos++;
    while (pos < json.size() && json[pos] == ' ') {
      pos++;
    }
    size_t end = pos;
    while (end < json.size() && json[end] >= '0' && json[end] <= '9') {
      end++;
    }
    if (end == pos) {
      return false;
    }
    *value = std::stoull(json.substr(pos, end - pos));
    return true;
  };

  size_t pos = json.find("\"source_id\"");
  if (pos == std::string::npos) {
    return false;
  }
  pos = json.find(':', pos);
  if (pos == std::string::npos) {
    return false;
  }
  pos = json.find('"', pos);
  if (pos == std::string::npos) {
    return false;
  }
  size_t end = json.find('"', pos + 1);
  if (end == std::string::npos) {
    return false;
  }
  std::string source_id = json.substr(pos + 1, end - pos - 1);
  if (source_id != source_id_) {
    return false;
  }

  uint64_t filenum = 0;
  uint64_t offset = 0;
  uint64_t logic_id = 0;
  uint64_t server_id = 0;
  uint64_t term_id = 0;
  uint64_t ts_ms = 0;
  if (!find_number("filenum", &filenum)) {
    return false;
  }
  if (!find_number("offset", &offset)) {
    return false;
  }
  find_number("logic_id", &logic_id);
  find_number("server_id", &server_id);
  find_number("term_id", &term_id);
  find_number("ts_ms", &ts_ms);

  out->filenum = static_cast<uint32_t>(filenum);
  out->offset = offset;
  out->logic_id = logic_id;
  out->server_id = static_cast<uint32_t>(server_id);
  out->term_id = static_cast<uint32_t>(term_id);
  out->ts_ms = ts_ms;
  return true;
}
