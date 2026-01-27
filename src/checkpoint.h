#ifndef CHECKPOINT_H_
#define CHECKPOINT_H_

#include <cstdint>
#include <mutex>
#include <string>

class Binlog;

#include <librdkafka/rdkafka.h>

struct Checkpoint {
  uint32_t filenum{0};
  uint64_t offset{0};
  uint64_t logic_id{0};
  uint32_t server_id{0};
  uint64_t ts_ms{0};
};

class CheckpointManager {
 public:
  CheckpointManager(std::string path, std::string source_id, std::string offsets_topic, std::string brokers);

  bool Load(Checkpoint* out);
  void SetBinlog(Binlog* binlog);
  void OnAck(rd_kafka_t* producer, const Checkpoint& cp);

 private:
  bool LoadFromFile(Checkpoint* out);
  bool LoadFromKafka(Checkpoint* out);
  void PersistLocal(const Checkpoint& cp);
  void PersistKafka(rd_kafka_t* producer, const Checkpoint& cp);
  bool IsNewer(const Checkpoint& cp) const;
  std::string CheckpointToJson(const Checkpoint& cp) const;
  bool ParseCheckpointJson(const std::string& json, Checkpoint* out) const;

 private:
  std::string path_;
  std::string source_id_;
  std::string offsets_topic_;
  std::string brokers_;

  mutable std::mutex mutex_;
  Checkpoint last_;
  bool has_last_{false};
  Binlog* binlog_{nullptr};
};

#endif  // CHECKPOINT_H_
