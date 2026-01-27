#ifndef KAFKA_SENDER_H_
#define KAFKA_SENDER_H_

#include <atomic>
#include <mutex>
#include <queue>
#include <string>

#include <librdkafka/rdkafka.h>

#include "checkpoint.h"
#include "conf.h"
#include "net/include/bg_thread.h"
#include "pstd/include/pstd_mutex.h"

struct KafkaRecord {
  std::string topic;
  std::string key;
  std::string payload;
  bool has_checkpoint{false};
  Checkpoint checkpoint;
};

class KafkaSender : public net::Thread {
 public:
  KafkaSender(int id, const Conf& conf, CheckpointManager* checkpoint_manager);
  ~KafkaSender() override;

  void Enqueue(const KafkaRecord& record);
  void Stop();
  int64_t elements() const { return elements_; }

 private:
  void* ThreadMain() override;
  bool InitProducer();
  void CloseProducer();

  static void DeliveryReportCallback(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque);

 private:
  struct DeliveryContext {
    CheckpointManager* checkpoint_manager;
    Checkpoint checkpoint;
    bool has_checkpoint;
  };

  int id_;
  Conf conf_;
  CheckpointManager* checkpoint_manager_;

  rd_kafka_t* producer_;
  std::atomic<bool> should_exit_;
  std::queue<KafkaRecord> queue_;
  pstd::Mutex queue_mutex_;
  pstd::CondVar queue_signal_;
  int64_t elements_;
};

#endif  // KAFKA_SENDER_H_
